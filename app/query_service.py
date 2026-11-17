"""查询端（独立进程，端口 8103）。

两种查询承诺（settings.query_promise），切换由控制台在满足闸门条件后执行：

- DUAL_WRITE_PENDING（默认）：迁移期承诺。派生列带状态返回，PENDING/STALE 永远
  显式呈现为状态对象而不是静默 null——调用方不可能把"尚未回填"误读成合法空值。
- DERIVED_AUTHORITATIVE：切换后承诺。此时闸门已保证全量对齐，可直接按派生列查询；
  若条件被破坏（例如切换后又有实时写入），/query/cutover-ready 会重新报红。

合法空值与未回填的区别在数据层即不可混淆：
  合法空值 = derived_status='COMPUTED' 且 derived_value IS NULL（规则确实算出 None）
  未回填   = derived_status='PENDING'/'STALE'，derived_value 即使有残留也不对外呈现
"""
from .db import connect, get_setting, init_schema, row_to_dict
from .http_util import HttpError, serve


def _derived_view(row, promise: str) -> dict:
    status = row["derived_status"]
    if status == "COMPUTED":
        return {
            "status": "COMPUTED",
            "value": row["derived_value"],   # 可能为 None —— 这才是合法空值
            "is_legal_null": row["derived_value"] is None,
            "rule_version": row["derived_rule_version"],
            "source_version": row["derived_from_version"],
            "computed_at": row["computed_at"],
        }
    # 迁移期：未回填/已过期必须显式表达，不返回裸 null
    return {
        "status": status,  # PENDING 或 STALE
        "value": None,
        "is_legal_null": False,
        "hint": "派生值尚未回填" if status == "PENDING"
                else "源行已更新，派生值待追平，旧值不得作为查询结果",
        "rule_version": row["derived_rule_version"],
        "source_version": row["derived_from_version"],
    }


def cutover_readiness(conn) -> dict:
    """切换承诺前的闸门检查。"""
    rule = conn.execute("SELECT MAX(version) AS v FROM rules").fetchone()["v"]
    counts = {
        "pending": conn.execute(
            "SELECT COUNT(*) c FROM members WHERE derived_status='PENDING'").fetchone()["c"],
        "stale": conn.execute(
            "SELECT COUNT(*) c FROM members WHERE derived_status='STALE'").fetchone()["c"],
        "unapplied_events": conn.execute(
            "SELECT COUNT(*) c FROM change_log WHERE applied=0").fetchone()["c"],
        "rule_mismatch": conn.execute(
            "SELECT COUNT(*) c FROM members WHERE derived_status='COMPUTED' "
            "AND derived_rule_version<>?", (rule,)).fetchone()["c"],
        "source_version_mismatch": conn.execute(
            "SELECT COUNT(*) c FROM members WHERE derived_status='COMPUTED' "
            "AND derived_from_version<>row_version").fetchone()["c"],
    }
    blockers = [name for name, n in counts.items() if n > 0]
    active_runs = conn.execute(
        "SELECT COUNT(*) c FROM backfill_runs WHERE status IN ('PENDING','RUNNING')"
    ).fetchone()["c"]
    if active_runs:
        blockers.append("backfill_running")
    return {
        "ready": not blockers,
        "blockers": blockers,
        "counts": counts,
        "active_rule_version": rule,
        "active_backfill_runs": active_runs,
    }


def route(method, path, query, body):
    conn = connect()
    if method == "GET" and path == "/query/health":
        return {"service": "query", "ok": True,
                "query_promise": get_setting(conn, "query_promise")}

    if method == "GET" and path == "/query/cutover-ready":
        return cutover_readiness(conn)

    if method == "GET" and path == "/query/members":
        promise = get_setting(conn, "query_promise")
        limit = min(int(query.get("limit", 50)), 200)
        offset = int(query.get("offset", 0))
        status_filter = query.get("status")
        where, params = "", []
        if status_filter:
            where = "WHERE derived_status=?"
            params.append(status_filter)
        rows = conn.execute(
            f"SELECT * FROM members {where} ORDER BY id LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
        total = conn.execute(
            f"SELECT COUNT(*) c FROM members {where}", params
        ).fetchone()["c"]
        return {
            "query_promise": promise,
            "total": total, "limit": limit, "offset": offset,
            "items": [
                {**{k: r[k] for k in r.keys() if k not in
                    ("derived_value", "derived_status", "derived_rule_version",
                     "derived_from_version", "computed_at")},
                 "derived": _derived_view(r, promise)}
                for r in rows
            ],
        }

    if method == "GET" and path.startswith("/query/members/"):
        member_id = int(path.rsplit("/", 1)[1])
        row = conn.execute("SELECT * FROM members WHERE id=?", (member_id,)).fetchone()
        if not row:
            raise HttpError(404, f"成员 {member_id} 不存在")
        result = row_to_dict(row)
        result["derived"] = _derived_view(row, get_setting(conn, "query_promise"))
        return result

    if method == "GET" and path.startswith("/query/lineage/"):
        member_id = int(path.rsplit("/", 1)[1])
        row = conn.execute("SELECT * FROM members WHERE id=?", (member_id,)).fetchone()
        if not row:
            raise HttpError(404, f"成员 {member_id} 不存在")
        import json
        audits = conn.execute(
            "SELECT * FROM derived_audit WHERE member_id=? ORDER BY seq", (member_id,)
        ).fetchall()
        return {
            "member_id": member_id,
            "current": row_to_dict(row),
            "lineage": [
                {
                    "audit_seq": a["seq"],
                    "computed_value": a["derived_value"],
                    "rule_version_at_compute": a["rule_version"],
                    "source_row_version_used": a["source_version"],
                    "source_snapshot": json.loads(a["source_snapshot_json"]),
                    "reason": a["reason"],
                    "backfill_run_id": a["run_id"],
                    "computed_at": a["created_at"],
                }
                for a in audits
            ],
        }

    raise HttpError(404, f"未知路由: {method} {path}")


def main():
    init_schema(connect())
    server = serve(route, 8103)
    print("query service listening on http://127.0.0.1:8103", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
