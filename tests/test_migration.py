"""管道核心语义的单元测试（标准库 unittest，无需第三方依赖）。

覆盖：
- 规则引擎：短路求值、None 安全、AST 注入拒绝
- 接入写入：row_version 推进、STALE 置位、change_log 入队
- 版本守卫：持快照期间并发更新必然 BLOCKED 且不写审计；事件追平后按新版本 COMPUTED
- 闸门：存在 PENDING/STALE/未应用事件时不可切换承诺
"""
import json
import os
import tempfile
import unittest


class MigrationTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        os.environ["MIG_DB"] = os.path.join(self.tmp.name, "test.db")
        from app.db import connect, init_schema
        from app.rule_engine import create_rule, evaluate  # 预热导入，避免计时窗口被 import 挤占
        self._evaluate = evaluate
        self.conn = connect()
        init_schema(self.conn)
        self.rule = create_rule(
            self.conn, name="r1",
            expr=("'VIP' if credit_score is not None and credit_score >= 720 "
                  "and balance >= 200000 else 'GROWTH' if credit_score is not None "
                  "and credit_score >= 600 else 'BASIC'"),
            description="test", activate=True,
        )

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()
        os.environ.pop("MIG_DB", None)


class RuleEngineTest(MigrationTestBase):
    def test_short_circuit_and_none_safety(self):
        from app.rule_engine import evaluate
        from app.ingest_service import upsert_member
        row = upsert_member(self.conn, 1, {
            "name": "无信用分", "credit_score": None, "balance": 500000})
        # None 参与比较前被 is not None 短路，不抛 TypeError
        self.assertEqual(evaluate(self.rule["expr"], row), "BASIC")

        row = upsert_member(self.conn, 2, {"credit_score": 750, "balance": 300000})
        self.assertEqual(evaluate(self.rule["expr"], row), "VIP")
        row = upsert_member(self.conn, 3, {"credit_score": 650, "balance": 0})
        self.assertEqual(evaluate(self.rule["expr"], row), "GROWTH")

    def test_dangerous_expressions_rejected(self):
        from app.rule_engine import RuleValidationError, validate_expr
        for bad in (
            "__import__('os').system('id')",
            "balance.__class__",
            "name[0]",
            "{k: 1 for k in (1,)}",
            "lambda: 1",
            "credit_score + unknown_field",
        ):
            with self.assertRaises(RuleValidationError):
                validate_expr(bad)


class IngestPathTest(MigrationTestBase):
    def test_version_advances_stale_and_change_log(self):
        from app.ingest_service import upsert_member
        row = upsert_member(self.conn, 7, {"credit_score": 700, "balance": 1000})
        self.assertEqual(row["row_version"], 1)
        self.assertEqual(row["derived_status"], "PENDING")
        row = upsert_member(self.conn, 7, {"balance": 2000})
        self.assertEqual(row["row_version"], 2)
        self.assertEqual(row["derived_status"], "STALE")
        events = self.conn.execute(
            "SELECT * FROM change_log WHERE member_id=7 ORDER BY seq").fetchall()
        self.assertEqual([e["op"] for e in events], ["INSERT", "UPDATE"])
        self.assertTrue(all(e["applied"] == 0 for e in events))
        payload = json.loads(events[-1]["payload_json"])
        self.assertEqual(payload["balance"], 2000)


class VersionGuardTest(MigrationTestBase):
    def test_concurrent_update_during_snapshot_gap_is_blocked(self):
        from app.ingest_service import upsert_member
        from app.worker import apply_compute, drain_events
        from app.db import connect as db_connect

        # 回填先读到 v1 快照（真实场景里这一步发生在竞争窗口开始前）
        upsert_member(self.conn, 42, {"credit_score": 700, "balance": 1000})
        seen_version = self.conn.execute(
            "SELECT row_version FROM members WHERE id=42").fetchone()["row_version"]
        self.assertEqual(seen_version, 1)

        # 回填仍持有 v1 快照期间，接入服务在另一连接把行推进到 v2
        other = db_connect()
        upsert_member(other, 42, {"balance": 999999})
        other.close()

        # 回填带着旧 seen_version=1 尝试写回 → 乐观守卫必须 BLOCKED
        result = apply_compute(
            self.conn, 42, seen_version=seen_version,
            expected_rule_version=self.rule["version"],
            rule=self.rule, reason="BACKFILL", run_id=None,
        )
        self.assertEqual(result, "BLOCKED", "旧快照必须被版本守卫拦截")

        row = self.conn.execute("SELECT * FROM members WHERE id=42").fetchone()
        self.assertEqual(row["row_version"], 2)
        self.assertEqual(row["derived_status"], "STALE")
        audits = self.conn.execute(
            "SELECT COUNT(*) c FROM derived_audit WHERE member_id=42").fetchone()
        self.assertEqual(audits["c"], 0, "被拦截的旧值不得留下审计/派生记录")

        # 事件追平：按最新快照（v2）计算并 COMPUTED
        stats = drain_events(self.conn, self.rule)
        self.assertGreaterEqual(stats["updated"], 1)
        row = self.conn.execute("SELECT * FROM members WHERE id=42").fetchone()
        self.assertEqual(row["derived_status"], "COMPUTED")
        self.assertEqual(row["derived_from_version"], 2)
        audit = self.conn.execute(
            "SELECT * FROM derived_audit WHERE member_id=42 ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        self.assertEqual(audit["source_version"], 2)
        self.assertEqual(json.loads(audit["source_snapshot_json"])["balance"], 999999)
        self.assertEqual(audit["reason"], "LIVE_CATCHUP")
        other.close()

    def test_idempotent_skip_when_already_fresh(self):
        from app.ingest_service import upsert_member
        from app.worker import apply_compute
        upsert_member(self.conn, 5, {"credit_score": 700})
        first = apply_compute(
            self.conn, 5, 1, self.rule["version"], self.rule, "BACKFILL", None)
        second = apply_compute(
            self.conn, 5, 1, self.rule["version"], self.rule, "BACKFILL", None)
        self.assertEqual(first, "UPDATED")
        self.assertEqual(second, "ALREADY_FRESH")


class CutoverGateTest(MigrationTestBase):
    def test_gate_blocks_until_fully_aligned(self):
        from app.ingest_service import upsert_member
        from app.query_service import cutover_readiness
        from app.worker import apply_compute
        from app.db import set_setting
        upsert_member(self.conn, 1, {"credit_score": 700})
        gate = cutover_readiness(self.conn)
        self.assertFalse(gate["ready"])
        self.assertIn("pending", gate["blockers"])

        apply_compute(self.conn, 1, 1, self.rule["version"], self.rule, "BACKFILL", None)
        # 还有未应用事件，闸门仍不放行
        gate = cutover_readiness(self.conn)
        self.assertIn("unapplied_events", gate["blockers"])
        self.conn.execute("UPDATE change_log SET applied=1")
        self.assertTrue(cutover_readiness(self.conn)["ready"])

        set_setting(self.conn, "query_promise", "DERIVED_AUTHORITATIVE")
        # 切换后再来实时写入 → 闸门重新报红
        upsert_member(self.conn, 1, {"balance": 1})
        gate = cutover_readiness(self.conn)
        self.assertFalse(gate["ready"])
        self.assertIn("stale", gate["blockers"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
