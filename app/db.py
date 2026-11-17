"""SQLite 连接、schema 初始化与通用辅助。

所有进程（接入服务 / 回填进程 / 查询端 / 控制台）共享同一个数据库文件，
靠 WAL + busy_timeout + 显式行级版本守卫协调并发，而不是靠单一进程内锁。
"""
import os
import sqlite3
from datetime import datetime, timezone

DEFAULT_DB = os.path.join(os.path.dirname(__file__), "..", "data", "migration.db")

SOURCE_FIELDS = (
    "name", "age", "city_tier", "credit_score",
    "balance", "active_days", "is_active",
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS rules (
  version      INTEGER PRIMARY KEY,
  name         TEXT NOT NULL,
  expr         TEXT NOT NULL,
  description  TEXT,
  active       INTEGER NOT NULL DEFAULT 0,
  created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS members (
  id                     INTEGER PRIMARY KEY,
  name                   TEXT,
  age                    INTEGER,
  city_tier              TEXT,
  credit_score           INTEGER,
  balance                INTEGER,
  active_days            INTEGER,
  is_active              INTEGER,
  row_version            INTEGER NOT NULL DEFAULT 1,
  updated_at             TEXT NOT NULL,
  -- 迁移三态：PENDING 未回填 / STALE 原值已变派生值过期 / COMPUTED 已对齐
  derived_status         TEXT NOT NULL DEFAULT 'PENDING'
                           CHECK (derived_status IN ('PENDING','STALE','COMPUTED')),
  derived_value          TEXT,
  derived_rule_version   INTEGER,
  derived_from_version   INTEGER,
  computed_at            TEXT
);

CREATE TABLE IF NOT EXISTS change_log (
  seq              INTEGER PRIMARY KEY AUTOINCREMENT,
  member_id        INTEGER NOT NULL,
  op               TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE')),
  new_row_version  INTEGER NOT NULL,
  payload_json     TEXT NOT NULL,
  applied          INTEGER NOT NULL DEFAULT 0,
  applied_at       TEXT,
  created_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_change_log_applied ON change_log(applied, seq);

CREATE TABLE IF NOT EXISTS derived_audit (
  seq                    INTEGER PRIMARY KEY AUTOINCREMENT,
  member_id              INTEGER NOT NULL,
  derived_value          TEXT,
  rule_version           INTEGER NOT NULL,
  source_version         INTEGER NOT NULL,
  source_snapshot_json   TEXT NOT NULL,
  reason                 TEXT NOT NULL CHECK (reason IN ('BACKFILL','LIVE_CATCHUP')),
  run_id                 INTEGER,
  created_at             TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_member ON derived_audit(member_id, seq);

CREATE TABLE IF NOT EXISTS backfill_runs (
  id                          INTEGER PRIMARY KEY AUTOINCREMENT,
  id_min                      INTEGER NOT NULL,
  id_max                      INTEGER NOT NULL,
  status                      TEXT NOT NULL
                                CHECK (status IN ('PENDING','RUNNING','DONE','INTERRUPTED')),
  rule_version_target         INTEGER NOT NULL,
  cursor_id                   INTEGER NOT NULL DEFAULT 0,
  total_rows                  INTEGER NOT NULL DEFAULT 0,
  processed_rows              INTEGER NOT NULL DEFAULT 0,
  computed_writes             INTEGER NOT NULL DEFAULT 0,
  stale_overwrites_blocked    INTEGER NOT NULL DEFAULT 0,
  note                        TEXT,
  started_at                  TEXT,
  updated_at                  TEXT,
  finished_at                 TEXT
);

CREATE TABLE IF NOT EXISTS settings (
  key    TEXT PRIMARY KEY,
  value  TEXT NOT NULL
);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or os.environ.get("MIG_DB") or DEFAULT_DB
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    if get_setting(conn, "query_promise") is None:
        set_setting(conn, "query_promise", "DUAL_WRITE_PENDING")


def get_setting(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO settings(key, value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}
