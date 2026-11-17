"""控制台服务（独立进程，端口 8101）：页面 + 编排 API。

职责：
- 规则版本管理（页面调整规则 → 新版本；可切换激活版本，旧版本血缘保留）
- 回填 run 的创建/中断/恢复，以子进程方式启动 worker，信号优雅停止
- 查询承诺切换，切换前强制走 cutover 闸门
- 成员修改统一代理到接入服务（保证所有写入都经过 row_version + change_log 路径）
- 托管实时写入模拟器
"""
import json
import os
import signal
import subprocess
import sys
import urllib.request

from .db import connect, get_setting, init_schema, row_to_dict, set_setting
from .http_util import HttpError, serve
from .query_service import cutover_readiness
from .rule_engine import (RuleValidationError, activate_rule, create_rule,
                          get_active_rule, list_rules)
from .simulator import IngestSimulator

INGEST_URL = "http://127.0.0.1:8102"
STATIC_DIR = os.path.join(os.path.dirname(__file__), "..", "web")

# run_id -> subprocess.Popen，仅本进程内存；worker 自身也会把中断状态持久化到库
_workers: dict[int, subprocess.Popen] = {}
_simulator = IngestSimulator()


def _ingest_post(path: str, payload: dict) -> dict:
    req = urllib.request.Request(
        INGEST_URL + path, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise HttpError(e.code, json.loads(e.read()).get("error", "接入服务错误"))


def _spawn_worker(run_id: int, batch_delay: float,
                  row_delay: float = 0.0, batch_size: int = 25,
                  snapshot_gap: float = 0.0) -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.worker", "--run-id", str(run_id),
         "--batch-delay", str(batch_delay), "--row-delay", str(row_delay),
         "--batch-size", str(batch_size), "--snapshot-gap", str(snapshot_gap)],
        cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
        stdout=open(os.path.join(os.path.dirname(__file__), "..", "data", f"worker_{run_id}.log"), "a"),
        stderr=subprocess.STDOUT,
    )
    _workers[run_id] = proc
    return proc


def _worker_alive(run_id: int) -> bool:
    proc = _workers.get(run_id)
    return proc is not None and proc.poll() is None


def _summary(conn) -> dict:
    status_rows = conn.execute(
        "SELECT derived_status, COUNT(*) c FROM members GROUP BY derived_status"
    ).fetchall()
    status_counts = {r["derived_status"]: r["c"] for r in status_rows}
    total = conn.execute("SELECT COUNT(*) c FROM members").fetchone()["c"]
    unapplied = conn.execute(
        "SELECT COUNT(*) c FROM change_log WHERE applied=0").fetchone()["c"]
    legal_nulls = conn.execute(
        "SELECT COUNT(*) c FROM members WHERE derived_status='COMPUTED' AND derived_value IS NULL"
    ).fetchone()["c"]
    runs = [dict(r) for r in conn.execute(
        "SELECT * FROM backfill_runs ORDER BY id DESC LIMIT 10").fetchall()]
    for r in runs:
        r["process_alive"] = _worker_alive(r["id"])
    return {
        "total_members": total,
        "status_counts": {
            "PENDING": status_counts.get("PENDING", 0),
            "STALE": status_counts.get("STALE", 0),
            "COMPUTED": status_counts.get("COMPUTED", 0),
        },
        "legal_null_computed": legal_nulls,
        "unapplied_events": unapplied,
        "query_promise": get_setting(conn, "query_promise"),
        "active_rule": get_active_rule(conn),
        "cutover": cutover_readiness(conn),
        "runs": runs,
        "simulator": _simulator.status(),
    }


def route(method, path, query, body):
    conn = connect()
    body = body or {}

    # ---------- 页面 ----------
    if method == "GET" and (path == "/" or path == "/index.html"):
        return ("static", "index.html")
    if method == "GET" and path.startswith("/web/"):
        return ("static", path[4:])

    # ---------- 总览 ----------
    if method == "GET" and path == "/api/state":
        return _summary(conn)

    # ---------- 规则 ----------
    if method == "GET" and path == "/api/rules":
        return {"items": list_rules(conn), "active": get_active_rule(conn)}

    if method == "POST" and path == "/api/rules":
        try:
            rule = create_rule(
                conn, body.get("name", "未命名规则"), body.get("expr", ""),
                body.get("description", ""), bool(body.get("activate", True)),
            )
        except RuleValidationError as e:
            raise HttpError(400, str(e))
        return rule

    if method == "POST" and path.startswith("/api/rules/") and path.endswith("/activate"):
        version = int(path.split("/")[-2])
        try:
            activate_rule(conn, version)
        except RuleValidationError as e:
            raise HttpError(404, str(e))
        return {"active_version": version}

    # ---------- 回填 run ----------
    if method == "GET" and path == "/api/runs":
        runs = [dict(r) for r in conn.execute(
            "SELECT * FROM backfill_runs ORDER BY id").fetchall()]
        for r in runs:
            r["process_alive"] = _worker_alive(r["id"])
        return {"items": runs}

    if method == "POST" and path == "/api/runs/start":
        id_min = int(body.get("id_min", 1))
        id_max = int(body.get("id_max", 500))
        batch_delay = float(body.get("batch_delay", 0.15))
        row_delay = float(body.get("row_delay", 0.0))
        batch_size = int(body.get("batch_size", 25))
        snapshot_gap = float(body.get("snapshot_gap", 0.0))
        if id_min > id_max:
            raise HttpError(400, "id_min 不能大于 id_max")
        active = get_active_rule(conn)
        if not active:
            raise HttpError(409, "没有激活规则，无法启动回填")
        running = conn.execute(
            "SELECT * FROM backfill_runs WHERE status IN ('PENDING','RUNNING')"
        ).fetchall()
        live = [r for r in running if _worker_alive(r["id"])]
        if live:
            raise HttpError(409, f"run {live[0]['id']} 仍在运行，请先中断")

        # 优先恢复中断的 run（同一范围），否则新建
        resume_target = conn.execute(
            "SELECT * FROM backfill_runs WHERE status='INTERRUPTED' "
            "AND id_min=? AND id_max=? ORDER BY id DESC LIMIT 1",
            (id_min, id_max),
        ).fetchone()
        from .db import utcnow
        if resume_target:
            run_id = resume_target["id"]
            conn.execute(
                "UPDATE backfill_runs SET status='PENDING', updated_at=? WHERE id=?",
                (utcnow(), run_id),
            )
            note = f"恢复 run {run_id}，从 cursor_id={resume_target['cursor_id']} 继续"
        else:
            cur = conn.execute(
                "INSERT INTO backfill_runs(id_min,id_max,status,rule_version_target,"
                "cursor_id,started_at,updated_at,note) VALUES(?,?, 'PENDING', ?, 0, ?, ?, ?)",
                (id_min, id_max, active["version"], utcnow(), utcnow(),
                 body.get("note", "页面创建的回填")),
            )
            run_id = cur.lastrowid
            note = f"新建 run {run_id}"
        proc = _spawn_worker(run_id, batch_delay, row_delay, batch_size, snapshot_gap)
        return {"run_id": run_id, "pid": proc.pid, "note": note}

    if method == "POST" and path.startswith("/api/runs/") and path.endswith("/stop"):
        run_id = int(path.split("/")[-2])
        proc = _workers.get(run_id)
        if proc is None or proc.poll() is not None:
            raise HttpError(409, "该 run 没有存活的回填进程（可能已自行结束）")
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=20)
        return {"stopped": run_id}

    # ---------- 成员（写操作代理接入服务，读直接查库展示三态） ----------

    # ---------- 成员（写操作代理接入服务，读直接查库展示三态） ----------
    if method == "GET" and path == "/api/members":
        limit = min(int(query.get("limit", 20)), 200)
        offset = int(query.get("offset", 0))
        status_filter = query.get("status")
        where, params = "", []
        if status_filter:
            where, params = "WHERE derived_status=?", [status_filter]
        rows = conn.execute(
            f"SELECT * FROM members {where} ORDER BY id LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        return {"items": [row_to_dict(r) for r in rows]}

    if method == "GET" and path.startswith("/api/members/"):
        member_id = int(path.rsplit("/", 1)[1])
        row = conn.execute("SELECT * FROM members WHERE id=?", (member_id,)).fetchone()
        if not row:
            raise HttpError(404, "成员不存在")
        return row_to_dict(row)

    if method == "POST" and path.startswith("/api/members/") and path.endswith("/fields"):
        member_id = int(path.split("/")[-2])
        if not body.get("fields"):
            raise HttpError(400, "fields 不能为空")
        return _ingest_post("/ingest/members", {"id": member_id, "fields": body["fields"]})

    if method == "GET" and path.startswith("/api/lineage/"):
        member_id = int(path.rsplit("/", 1)[1])
        row = conn.execute("SELECT * FROM members WHERE id=?", (member_id,)).fetchone()
        if not row:
            raise HttpError(404, "成员不存在")
        audits = [dict(a) for a in conn.execute(
            "SELECT * FROM derived_audit WHERE member_id=? ORDER BY seq", (member_id,)
        ).fetchall()]
        for a in audits:
            snapshot = json.loads(a.pop("source_snapshot_json"))
            a["source_snapshot"] = snapshot
            a["audit_seq"] = a.pop("seq")
            a["computed_value"] = a.pop("derived_value")
            a["rule_version_at_compute"] = a.pop("rule_version")
            a["source_row_version_used"] = a.pop("source_version")
            a["backfill_run_id"] = a.pop("run_id")
            a["computed_at"] = a.pop("created_at")
        events = [dict(e) for e in conn.execute(
            "SELECT seq,op,new_row_version,applied,created_at FROM change_log "
            "WHERE member_id=? ORDER BY seq", (member_id,)).fetchall()]
        return {"member": row_to_dict(row), "lineage": audits, "change_events": events}

    # ---------- 查询承诺切换 ----------
    if method == "GET" and path == "/api/cutover-ready":
        return cutover_readiness(conn)

    if method == "POST" and path == "/api/promise":
        value = body.get("value")
        if value not in ("DUAL_WRITE_PENDING", "DERIVED_AUTHORITATIVE"):
            raise HttpError(400, "非法承诺值")
        if value == "DERIVED_AUTHORITATIVE":
            readiness = cutover_readiness(conn)
            if not readiness["ready"]:
                raise HttpError(
                    409, f"闸门未通过，阻塞项: {', '.join(readiness['blockers'])}；"
                         f"明细: {readiness['counts']}"
                )
        set_setting(conn, "query_promise", value)
        return {"query_promise": value, "cutover": cutover_readiness(conn)}

    # ---------- 模拟器 ----------
    if method == "POST" and path == "/api/simulator/start":
        _simulator.id_min = int(body.get("id_min", 1))
        _simulator.id_max = int(body.get("id_max", 500))
        _simulator.interval = float(body.get("interval", 0.25))
        _simulator.hot_ids = [int(x) for x in body.get("hot_ids", [])]
        _simulator.start()
        return _simulator.status()
    if method == "POST" and path == "/api/simulator/stop":
        _simulator.stop()
        return _simulator.status()
    if method == "GET" and path == "/api/simulator":
        return _simulator.status()

    raise HttpError(404, f"未知路由: {method} {path}")


def main():
    init_schema(connect())
    server = serve(route, 8101, static_dir=STATIC_DIR)
    print("control console listening on http://127.0.0.1:8101", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        _simulator.stop()
        server.shutdown()


if __name__ == "__main__":
    main()
