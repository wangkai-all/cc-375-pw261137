"""接入服务（独立进程，端口 8102）。

职责边界：
- 只写源列 + row_version + 时间戳；绝不自己计算派生列（派生策略=先存变更再追平）。
- 每次 INSERT/UPDATE 追加一条 change_log（未应用事件），回填进程/实时追平据此重算。
- 行被修改的瞬间把 derived_status 置 STALE，使慢回填的旧快照无法通过版本守卫，
  查询端也不会把"派生值过期"当成合法结果展示。
"""
import json

from .db import SOURCE_FIELDS, connect, get_setting, init_schema, row_to_dict, utcnow
from .http_util import HttpError, serve


def _payload_for(row) -> dict:
    return {f: row[f] for f in SOURCE_FIELDS}


def upsert_member(conn, member_id: int, fields: dict, op: str | None = None) -> dict:
    unknown = set(fields) - set(SOURCE_FIELDS)
    if unknown:
        raise HttpError(400, f"未知字段: {sorted(unknown)}")
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute("SELECT * FROM members WHERE id=?", (member_id,)).fetchone()
        now = utcnow()
        if existing is None:
            defaults = {f: None for f in SOURCE_FIELDS}
            defaults.update(fields)
            cols = ["id"] + list(SOURCE_FIELDS) + ["row_version", "updated_at", "derived_status"]
            placeholders = ",".join("?" for _ in cols)
            conn.execute(
                f"INSERT INTO members({','.join(cols)}) VALUES({placeholders})",
                [member_id] + [defaults[f] for f in SOURCE_FIELDS] + [1, now, "PENDING"],
            )
            new_version, real_op = 1, "INSERT"
        else:
            # 局部更新：未提供的字段保持原值
            new_version = existing["row_version"] + 1
            sets = [f"{f}=?" for f in fields]
            params = [fields[f] for f in fields]
            if sets:
                conn.execute(
                    f"UPDATE members SET {','.join(sets)}, row_version=?, updated_at=?, "
                    f"derived_status='STALE' WHERE id=?",
                    params + [new_version, now, member_id],
                )
            real_op = op or "UPDATE"
        row = conn.execute("SELECT * FROM members WHERE id=?", (member_id,)).fetchone()
        # 变更事件：回填/追平消费后把 applied 置 1
        conn.execute(
            "INSERT INTO change_log(member_id,op,new_row_version,payload_json,applied,created_at) "
            "VALUES(?,?,?,? ,0,?)",
            (member_id, real_op, new_version, json.dumps(_payload_for(row), ensure_ascii=False), now),
        )
        conn.execute("COMMIT")
        return row_to_dict(conn.execute("SELECT * FROM members WHERE id=?", (member_id,)).fetchone())
    except Exception:
        conn.execute("ROLLBACK")
        raise


def route(method, path, query, body):
    conn = connect()
    if method == "GET" and path == "/ingest/health":
        return {"service": "ingest", "ok": True, "query_promise": get_setting(conn, "query_promise")}

    if method == "POST" and path == "/ingest/members":
        body = body or {}
        if "id" not in body:
            raise HttpError(400, "缺少 id")
        return upsert_member(conn, int(body["id"]), body.get("fields", {}))

    if method == "POST" and path == "/ingest/members/bulk":
        body = body or {}
        out = []
        for item in body.get("items", []):
            out.append(upsert_member(conn, int(item["id"]), item.get("fields", {})))
        return {"upserted": len(out), "items": out}

    if method == "GET" and path.startswith("/ingest/members/"):
        member_id = int(path.rsplit("/", 1)[1])
        row = conn.execute("SELECT * FROM members WHERE id=?", (member_id,)).fetchone()
        if not row:
            raise HttpError(404, f"成员 {member_id} 不存在")
        return row_to_dict(row)

    raise HttpError(404, f"未知路由: {method} {path}")


def main():
    init_schema(connect())
    server = serve(route, 8102)
    print("ingest service listening on http://127.0.0.1:8102", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
