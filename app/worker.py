"""回填进程（独立进程）。

两条工作循环共用同一条"带版本守卫的计算写入"路径：

1) drain_events：消费 change_log 中 applied=0 的实时变更（LIVE_CATCHUP），
   保证接入服务的新写入即使在慢回填期间也能被追平，而不是被回填旧值盖掉。
2) backfill 扫描：按 id 游标处理指定范围内所有未对齐当前规则版本的行（BACKFILL）。

版本守卫（防旧原值覆盖新值的关键）：
  UPDATE ... WHERE id=? AND row_version=? AND derived_rule_version IS ?
  若 row_version 已被接入服务推进，0 行命中 → 计入 stale_overwrites_blocked，
  交由事件循环按最新快照重算，旧快照直接丢弃。

中断/恢复：run 行状态机 PENDING→RUNNING→(DONE|INTERRUPTED)，进度持久化在
  cursor_id / 统计列，同一 run 重启后从断点继续；规则换版后自动抬高目标版本。
"""
import argparse
import json
import signal
import time

from .db import SOURCE_FIELDS, connect, init_schema, row_to_dict, utcnow
from .rule_engine import RuleEvalError, get_active_rule

_STOP = False


def _request_stop(signum, frame):
    global _STOP
    _STOP = True


def _snapshot(row) -> dict:
    return {f: row[f] for f in SOURCE_FIELDS}


def apply_compute(conn, member_id: int, seen_version: int, expected_rule_version: int,
                  rule: dict, reason: str, run_id: int | None,
                  snapshot_gap: float = 0.0) -> str:
    """对单条记录执行带守卫的计算写入。

    snapshot_gap：读取快照之后、守卫写入之前刻意停顿的秒数，
    把"回填持有旧原值"与"写回派生值"之间的窗口显式化——
    接入服务在窗口内推进 row_version 时，本次写入必然被守卫拦截。

    返回:
      UPDATED        计算并写入了派生值（含合法 NULL）
      ALREADY_FRESH  无需处理（已按当前规则版本对齐该源行版本）
      BLOCKED        版本守卫拦截：源行已被接入服务推进，旧快照作废
      ERROR          规则求值失败（记录保持原状态，不写半成品）
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT * FROM members WHERE id=?", (member_id,)).fetchone()
        if row is None:
            conn.execute("ROLLBACK")
            return "BLOCKED"
        # 守卫零：调用方（扫描批次/事件）持有的快照版本必须未被接入服务推进。
        # 注意绝不能用当前行版本覆盖 seen_version——否则乐观守卫会形同虚设。
        if row["row_version"] != seen_version:
            conn.execute("ROLLBACK")
            return "BLOCKED"
        try:
            from .rule_engine import evaluate
            value = evaluate(rule["expr"], row)
        except RuleEvalError:
            conn.execute("ROLLBACK")
            return "ERROR"

        # 显式竞争窗口：基于旧快照算好派生值后，释放写锁并停顿，
        # 模拟真实慢回填的计算/IO 耗时。接入服务在窗口内可以立即推进
        # row_version；重新拿锁后重新读行，由乐观守卫决定放弃还是写回。
        if snapshot_gap:
            conn.execute("ROLLBACK")
            time.sleep(snapshot_gap)
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM members WHERE id=?", (member_id,)).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                return "BLOCKED"

        # 守卫一（停顿后最终判定）：竞争窗口内源行若被推进则放弃旧快照
        if row["row_version"] != seen_version:
            conn.execute("ROLLBACK")
            return "BLOCKED"
        # 守卫二：已经按同一规则版本对齐过这一版源行 → 幂等跳过
        if (row["derived_status"] == "COMPUTED"
                and row["derived_rule_version"] == expected_rule_version
                and row["derived_from_version"] == seen_version):
            conn.execute("ROLLBACK")
            return "ALREADY_FRESH"

        # 守卫三：UPDATE 本身携带 row_version 条件，rowcount 兜底防并发覆盖
        cur = conn.execute(
            "UPDATE members SET derived_status='COMPUTED', derived_value=?, "
            "derived_rule_version=?, derived_from_version=?, computed_at=? "
            "WHERE id=? AND row_version=?",
            (None if value is None else str(value), expected_rule_version, seen_version,
             utcnow(), member_id, seen_version),
        )
        if cur.rowcount != 1:
            conn.execute("ROLLBACK")
            return "BLOCKED"

        conn.execute(
            "INSERT INTO derived_audit(member_id,derived_value,rule_version,"
            "source_version,source_snapshot_json,reason,run_id,created_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (member_id, None if value is None else str(value), expected_rule_version,
             seen_version, json.dumps(_snapshot(row), ensure_ascii=False),
             reason, run_id, utcnow()),
        )
        conn.execute("COMMIT")
        return "UPDATED"
    except Exception:
        conn.execute("ROLLBACK")
        raise


def drain_events(conn, rule: dict, run_id: int | None = None, limit: int = 100,
                 id_min: int | None = None, id_max: int | None = None) -> dict:
    """消费未应用的实时变更事件，逐事件追平。

    id_min/id_max 给出回填 run 负责的 id 段：run 只追平自己范围内的事件，
    范围外成员的事件不会让本 run 的完成判定反复回绕。
    """
    stats = {"processed": 0, "updated": 0, "blocked": 0, "errors": 0}
    sql = "SELECT * FROM change_log WHERE applied=0"
    params: list = []
    if id_min is not None:
        sql += " AND member_id BETWEEN ? AND ?"
        params.extend([id_min, id_max])
    sql += " ORDER BY seq LIMIT ?"
    params.append(limit)
    events = conn.execute(sql, params).fetchall()
    for ev in events:
        if _STOP:
            break
        result = apply_compute(
            conn, ev["member_id"], ev["new_row_version"], rule["version"],
            rule, "LIVE_CATCHUP", run_id,
        )
        if result in ("UPDATED", "ALREADY_FRESH"):
            conn.execute(
                "UPDATE change_log SET applied=1, applied_at=? WHERE seq=? AND applied=0",
                (utcnow(), ev["seq"]),
            )
            stats["processed"] += 1
            if result == "UPDATED":
                stats["updated"] += 1
        elif result == "BLOCKED":
            # 事件本身版本落后于行当前版本：它已被后续事件取代，标记应用并等下一事件处理
            current = conn.execute(
                "SELECT row_version FROM members WHERE id=?", (ev["member_id"],)
            ).fetchone()
            if current and current["row_version"] > ev["new_row_version"]:
                conn.execute(
                    "UPDATE change_log SET applied=1, applied_at=? WHERE seq=?",
                    (utcnow(), ev["seq"]),
                )
                stats["processed"] += 1
            stats["blocked"] += 1
        else:
            stats["errors"] += 1
    return stats


def _needs_work(row, rule_version: int) -> bool:
    return not (row["derived_status"] == "COMPUTED"
                and row["derived_rule_version"] == rule_version
                and row["derived_from_version"] == row["row_version"])


def run_backfill(conn, run_id: int, batch_delay: float,
                 row_delay: float = 0.0, batch_size: int = 25,
                 snapshot_gap: float = 0.0) -> None:
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    run = conn.execute("SELECT * FROM backfill_runs WHERE id=?", (run_id,)).fetchone()
    if run is None:
        raise SystemExit(f"backfill run {run_id} 不存在")
    if run["status"] == "DONE":
        print(f"[worker] run {run_id} 已完成，无需重启", flush=True)
        return

    rule = get_active_rule(conn)
    if rule is None:
        raise SystemExit("没有可用的激活规则")

    # 恢复或抬升目标规则版本：运行期间规则换版，老 run 自动指向新版本继续追
    target_rule = max(rule["version"], run["rule_version_target"])
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "UPDATE backfill_runs SET status='RUNNING', rule_version_target=?, started_at=COALESCE(started_at,?), "
        "updated_at=? WHERE id=?",
        (target_rule, utcnow(), utcnow(), run_id),
    )
    total = conn.execute(
        "SELECT COUNT(*) AS c FROM members WHERE id BETWEEN ? AND ?",
        (run["id_min"], run["id_max"]),
    ).fetchone()["c"]
    conn.execute("UPDATE backfill_runs SET total_rows=? WHERE id=?", (total, run_id))
    conn.execute("COMMIT")

    print(f"[worker] run {run_id} 启动，id 范围 [{run['id_min']},{run['id_max']}]，"
          f"目标规则 v{target_rule}，共 {total} 行", flush=True)

    cursor = run["cursor_id"]
    while not _STOP:
        rule = get_active_rule(conn)
        target_rule = max(rule["version"], target_rule)

        # 关键设计：前向扫描阶段不消费 change_log 事件。
        # 实时更新先把 row_version 推高（行置 STALE），扫描批次里持有的仍是旧快照，
        # 写入时被 row_version 守卫拦截并计入 stale_overwrites_blocked；
        # 事件统一在游标到底后的"追平阶段"按最新快照计算（reason=LIVE_CATCHUP）。
        # 这正是"慢回填不能用旧原值盖掉新原值"的完整交接链路。
        batch = conn.execute(
            "SELECT * FROM members WHERE id BETWEEN ? AND ? AND id > ? "
            "ORDER BY id LIMIT ?",
            (run["id_min"], run["id_max"], cursor, batch_size),
        ).fetchall()

        blocked_in_batch = 0
        writes_in_batch = 0
        if batch:
            for row in batch:
                if _STOP:
                    break
                result = apply_compute(
                    conn, row["id"], row["row_version"], target_rule,
                    rule, "BACKFILL", run_id, snapshot_gap=snapshot_gap,
                )
                if result == "UPDATED":
                    writes_in_batch += 1
                elif result == "BLOCKED":
                    blocked_in_batch += 1
                elif result == "ERROR":
                    print(f"[worker] 成员 {row['id']} 规则求值失败，跳过待人工处理", flush=True)
                # 逐行停顿放大"扫描→写入"窗口：实时更新落在窗口内即触发版本守卫
                if row_delay and not _STOP:
                    time.sleep(row_delay)

            cursor = batch[-1]["id"]
            conn.execute(
                "UPDATE backfill_runs SET cursor_id=?, processed_rows=processed_rows+?, "
                "computed_writes=computed_writes+?, stale_overwrites_blocked=stale_overwrites_blocked+?, "
                "rule_version_target=?, updated_at=? WHERE id=?",
                (cursor, len(batch), writes_in_batch, blocked_in_batch,
                 target_rule, utcnow(), run_id),
            )
            if writes_in_batch or blocked_in_batch:
                print(f"[worker] run {run_id} 游标={cursor} 本批写入={writes_in_batch} "
                      f"拦截旧值={blocked_in_batch}", flush=True)
            if batch_delay:
                time.sleep(batch_delay)
            continue

        # ---------- 游标到底：进入追平 / 收尾阶段 ----------
        ev_stats = drain_events(conn, rule, run_id=run_id,
                                id_min=run["id_min"], id_max=run["id_max"])
        if ev_stats["processed"]:
            print(f"[worker] run {run_id} 追平阶段消费实时事件 {ev_stats['processed']} 条"
                  f"（新值写入 {ev_stats['updated']}，被后续事件取代 {ev_stats['blocked']}）",
                  flush=True)

        remaining_events = conn.execute(
            "SELECT COUNT(*) AS c FROM change_log WHERE applied=0 "
            "AND member_id BETWEEN ? AND ?",
            (run["id_min"], run["id_max"]),
        ).fetchone()["c"]
        if remaining_events > 0:
            # 追平期间又有新事件进来，继续消费直到归零
            time.sleep(0.1)
            continue

        conn.execute("BEGIN IMMEDIATE")
        stale = conn.execute(
            "SELECT * FROM members WHERE id BETWEEN ? AND ?",
            (run["id_min"], run["id_max"]),
        ).fetchall()
        todo = [r for r in stale if _needs_work(r, target_rule)]
        if not todo:
            conn.execute(
                "UPDATE backfill_runs SET status='DONE', finished_at=?, updated_at=? "
                "WHERE id=?",
                (utcnow(), utcnow(), run_id),
            )
            conn.execute("COMMIT")
            print(f"[worker] run {run_id} 完成 ✅", flush=True)
            return
        conn.execute("ROLLBACK")
        # 换版等原因导致已扫过的行需要重算：游标回到范围起点再扫一遍
        cursor = run["id_min"] - 1
        print(f"[worker] 检测到 {len(todo)} 行需按新规则重算，游标回绕", flush=True)

    # 收到中断信号：持久化断点，保留 RUN 记录供恢复
    conn.execute(
        "UPDATE backfill_runs SET status='INTERRUPTED', cursor_id=?, updated_at=? WHERE id=?",
        (cursor, utcnow(), run_id),
    )
    print(f"[worker] run {run_id} 收到中断，断点保存在 cursor_id={cursor}，可随时恢复", flush=True)


def main():
    parser = argparse.ArgumentParser(description="回填进程")
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--batch-delay", type=float, default=0.15,
                        help="每批之间的暂停秒数，便于演示慢回填")
    parser.add_argument("--row-delay", type=float, default=0.0,
                        help="每行写入之间的暂停秒数，放大版本守卫的竞争窗口")
    parser.add_argument("--snapshot-gap", type=float, default=0.0,
                        help="读取旧快照后、守卫写入前的停顿秒数，确定性复现旧值拦截")
    parser.add_argument("--batch-size", type=int, default=25,
                        help="单批扫描行数，调小可制造更窄的扫描窗口")
    parser.add_argument("--once", action="store_true",
                        help="只跑事件追平一轮后退出（无 run 时的守护模式备用）")
    args = parser.parse_args()

    init_schema(connect())
    conn = connect()
    run_backfill(conn, args.run_id, args.batch_delay,
                 row_delay=args.row_delay, batch_size=args.batch_size,
                 snapshot_gap=args.snapshot_gap)


if __name__ == "__main__":
    main()
