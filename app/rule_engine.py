"""派生规则引擎：AST 白名单求值，杜绝 eval/exec 注入。

规则是存库的表达式字符串（rules.expr），页面可调整。每次调整产生新规则版本，
回填进程按新版本重算，旧版本计算记录保留在 derived_audit 中供血缘回溯。
"""
import ast
from typing import Any

from .db import SOURCE_FIELDS

# 仅放行纯数据运算需要的 AST 节点；属性访问、调用、下标、推导式一律拒绝
_ALLOWED_NODES = (
    ast.Expression, ast.BoolOp, ast.UnaryOp, ast.BinOp, ast.Compare, ast.IfExp,
    ast.Constant, ast.Name, ast.Load,
    ast.And, ast.Or, ast.Not,
    ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE, ast.Is, ast.IsNot,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod,
)


class RuleValidationError(ValueError):
    pass


class RuleEvalError(RuntimeError):
    def __init__(self, member_id: int, cause: Exception):
        super().__init__(f"member {member_id} rule eval failed: {cause}")
        self.member_id = member_id
        self.cause = cause


def validate_expr(expr: str) -> None:
    """规则创建时调用：语法 + 节点白名单 + 试求值。"""
    if not expr or not expr.strip():
        raise RuleValidationError("表达式为空")
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise RuleValidationError(f"语法错误: {e.msg}") from e
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise RuleValidationError(
                f"禁止的语法节点: {type(node).__name__}，仅允许比较/算术/布尔/条件表达式"
            )
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id not in SOURCE_FIELDS:
            raise RuleValidationError(f"未知字段名: {node.id}，可用字段: {', '.join(SOURCE_FIELDS)}")


def _eval_node(node: ast.AST, variables: dict[str, Any]) -> Any:
    if isinstance(node, ast.Expression):
        return _eval_node(node.body, variables)
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return variables[node.id]
    if isinstance(node, ast.BoolOp):
        # 严格复刻 Python 短路语义：and 遇假即停、or 遇真即停，
        # 保证 "x is not None and x > 0" 这类空值守卫表达式安全
        if isinstance(node.op, ast.And):
            result = True
            for v in node.values:
                result = _eval_node(v, variables)
                if not result:
                    return result
            return result
        result = False
        for v in node.values:
            result = _eval_node(v, variables)
            if result:
                return result
        return result
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return not _eval_node(node.operand, variables)
    if isinstance(node, ast.BinOp):
        left, right = _eval_node(node.left, variables), _eval_node(node.right, variables)
        ops = {
            ast.Add: lambda a, b: a + b, ast.Sub: lambda a, b: a - b,
            ast.Mult: lambda a, b: a * b, ast.Div: lambda a, b: a / b,
            ast.FloorDiv: lambda a, b: a // b, ast.Mod: lambda a, b: a % b,
        }
        return ops[type(node.op)](left, right)
    if isinstance(node, ast.Compare):
        left = _eval_node(node.left, variables)
        for op, comparator in zip(node.ops, node.comparators):
            right = _eval_node(comparator, variables)
            cmp = {
                ast.Eq: lambda a, b: a == b, ast.NotEq: lambda a, b: a != b,
                ast.Lt: lambda a, b: a < b, ast.LtE: lambda a, b: a <= b,
                ast.Gt: lambda a, b: a > b, ast.GtE: lambda a, b: a >= b,
                ast.Is: lambda a, b: a is b, ast.IsNot: lambda a, b: a is not b,
            }[type(op)]
            if not cmp(left, right):
                return False
            left = right
        return True
    if isinstance(node, ast.IfExp):
        if _eval_node(node.test, variables):
            return _eval_node(node.body, variables)
        return _eval_node(node.orelse, variables)
    raise RuleValidationError(f"禁止的语法节点: {type(node).__name__}")


def evaluate(expr: str, member_row) -> Any:
    """对一行成员数据求值。member_row 可以是 sqlite3.Row 或 dict。"""
    variables = {field: member_row[field] for field in SOURCE_FIELDS}
    try:
        tree = ast.parse(expr, mode="eval")
        return _eval_node(tree, variables)
    except RuleValidationError:
        raise
    except Exception as e:  # 除零、类型错误等带上成员上下文抛出
        raise RuleEvalError(member_row["id"], e)


def get_active_rule(conn) -> dict | None:
    row = conn.execute(
        "SELECT * FROM rules WHERE active=1 ORDER BY version DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def get_rule(conn, version: int) -> dict | None:
    row = conn.execute("SELECT * FROM rules WHERE version=?", (version,)).fetchone()
    return dict(row) if row else None


def list_rules(conn) -> list[dict]:
    return [dict(r) for r in conn.execute("SELECT * FROM rules ORDER BY version").fetchall()]


def create_rule(conn, name: str, expr: str, description: str, activate: bool) -> dict:
    """在同一事务内校验、落版本、（可选）切换 active 指针。"""
    validate_expr(expr)
    conn.execute("BEGIN IMMEDIATE")
    try:
        next_version = conn.execute("SELECT COALESCE(MAX(version),0)+1 AS v FROM rules").fetchone()["v"]
        from .db import utcnow
        conn.execute(
            "INSERT INTO rules(version,name,expr,description,active,created_at) VALUES(?,?,?,?,?,?)",
            (next_version, name, expr, description, 1 if activate else 0, utcnow()),
        )
        if activate:
            conn.execute("UPDATE rules SET active=0 WHERE version<>?", (next_version,))
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return get_rule(conn, next_version)


def activate_rule(conn, version: int) -> None:
    if get_rule(conn, version) is None:
        raise RuleValidationError(f"规则版本 {version} 不存在")
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("UPDATE rules SET active=0")
        conn.execute("UPDATE rules SET active=1 WHERE version=?", (version,))
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
