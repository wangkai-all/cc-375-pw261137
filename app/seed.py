"""生成虚构样例数据：500 名成员的历史表，全部处于 PENDING（迁移开始前的状态）。

场景设定：
  members 是存量历史表，新列 member_segment（客户分层）要由 credit_score /
  balance / active_days / age / is_active 多字段推导。部分成员 credit_score
  为 NULL 或 age < 18，作为规则边界输入；页面调整规则加入 "None if ... else ..."
  分支后即可演示"规则算出的合法空值"与"未回填 PENDING"的本质区别。
"""
import random

from .db import connect, init_schema, utcnow
from .rule_engine import create_rule

RULE_V1_EXPR = (
    "'VIP' if credit_score is not None and credit_score >= 720 and balance >= 200000 "
    "else 'GROWTH' if credit_score is not None and credit_score >= 600 "
    "else 'BASIC'"
)

RULE_V2_EXPR = (
    "'VIP' if credit_score is not None and credit_score >= 720 and balance >= 200000 "
    "and is_active == 1 "
    "else 'DORMANT' if is_active == 0 or active_days < 10 "
    "else 'GROWTH' if credit_score is not None and credit_score >= 600 and age >= 18 "
    "else 'BASIC'"
)

CITIES = ("TIER1", "TIER2", "TIER3")
SURNAMES = ("赵", "钱", "孙", "李", "周", "吴", "郑", "王", "冯", "陈")
GIVEN = ("伟", "芳", "娜", "敏", "静", "磊", "洋", "勇", "艳", "杰", "涛", "明")


def seed(db_path: str | None = None, n: int = 500, reset: bool = True) -> None:
    random.seed(20261117)
    conn = connect(db_path)
    init_schema(conn)
    if reset:
        for table in ("derived_audit", "change_log", "backfill_runs", "members", "rules", "settings"):
            conn.execute(f"DELETE FROM {table}")
        conn.execute(
            "INSERT INTO settings(key,value) VALUES('query_promise','DUAL_WRITE_PENDING') "
            "ON CONFLICT(key) DO UPDATE SET value='DUAL_WRITE_PENDING'"
        )

    # 规则 v1：迁移启动时的现行规则
    create_rule(
        conn, name="客户分层-v1", expr=RULE_V1_EXPR,
        description="按信用分与余额三档分层（迁移基线版本）", activate=True,
    )

    now = utcnow()
    rows = []
    for i in range(1, n + 1):
        # 约 8% 的成员没有信用分 → 规则分支产生合法空值场景的对照数据
        credit_score = None if random.random() < 0.08 else random.randint(450, 850)
        rows.append((
            i,
            random.choice(SURNAMES) + random.choice(GIVEN),
            random.randint(16, 72),
            random.choice(CITIES),
            credit_score,
            random.randint(-20000, 900000),
            random.randint(0, 365),
            random.choice([0, 1, 1, 1]),
            1, now, "PENDING",
        ))
    conn.executemany(
        "INSERT INTO members(id,name,age,city_tier,credit_score,balance,active_days,"
        "is_active,row_version,updated_at,derived_status) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    print(f"seed 完成：{n} 名成员（全部 PENDING），规则 v1 已激活；"
          f"含 {sum(1 for r in rows if r[4] is None)} 条 credit_score=NULL 边界数据")


if __name__ == "__main__":
    seed()
