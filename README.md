# 在线派生列数据迁移管道

历史表新增一个由**多个源字段推导**的派生列，要求：回填期间原记录仍被实时更新、
接入服务 / 回填进程 / 查询端三个进程独立运行；慢回填不能用旧原值盖掉新值，
也不能把"尚未回填"伪装成合法空值；规则可在页面调整、回填可指定范围 / 中断 / 恢复，
且能回溯每个新值由哪一版原值、哪一版规则计算。

本项目用 Python 标准库（sqlite3 + http.server，零第三方依赖）从零实现整条管道，
**不使用任何一次性 SQL 跑完迁移**——所有交接动作都通过服务接口与可中断的回填进程完成。

## 架构：三个独立进程 + 一个 SQLite(WAL) 库

| 进程 | 端口 | 职责 |
| --- | --- | --- |
| 控制台 `app/control_service.py` | 8101 | 页面与编排 API：规则版本、回填 run 子进程、承诺切换、模拟器 |
| 接入服务 `app/ingest_service.py` | 8102 | 唯一的源数据写入口：写源列、推进 `row_version`、置 STALE、追加 `change_log` |
| 查询端 `app/query_service.py` | 8103 | 三态查询、承诺闸门检查、血缘接口 |
| 回填 worker `app/worker.py` | 子进程 | 按 id 游标扫描回填 + 游标到底后消费 `change_log` 追平，可中断/恢复 |

## 关键设计决策

### 1. 派生策略：先存变更再追平（change-log catch-up），而非写入时同步计算

接入服务**只**写源字段，绝不自己算派生列。每次 INSERT/UPDATE：
`row_version += 1`、`derived_status='STALE'`、追加一条 `change_log(applied=0)`。
回填进程在扫描批次结束后的"追平阶段"按最新事件重算。这样接入路径不依赖规则引擎，
规则换版不需要改动写入端，慢回填也永远不会把派生列写回源表。

### 2. 乐观版本守卫：旧原值盖不掉新原值（核心）

`worker.apply_compute` 的每次写入都带三重守卫：

1. 读取快照时校验调用方持有的 `seen_version` 与当前 `row_version` 一致；
2. 持快照停顿（`--snapshot-gap`，模拟真实计算/IO 耗时）后重新读行，再次复核；
3. `UPDATE ... WHERE id=? AND row_version=?` 的 `rowcount` 兜底。

任一环节发现源行已被接入服务推进 → 返回 `BLOCKED`、计入
`backfill_runs.stale_overwrites_blocked`，旧快照直接丢弃，等待事件追平按新值重算。
被拦截的写入不会留下任何派生值或审计记录。

### 3. 三态模型：未回填 ≠ 合法空值

| 状态 | 含义 | 查询端呈现 |
| --- | --- | --- |
| `PENDING` | 从未回填 | 显式状态对象 + hint，**绝不返回裸 null** |
| `STALE` | 源行已更新、派生值过期 | 显式状态对象，提示旧值不得作为查询结果 |
| `COMPUTED` + value=NULL | 规则确实算出 None | `is_legal_null=true` 的合法空值 |
| `COMPUTED` + value=... | 已对齐 | 正常派生值 |

### 4. 查询承诺切换闸门

`settings.query_promise` 两档：`DUAL_WRITE_PENDING`（默认，迁移期三态显式）与
`DERIVED_AUTHORITATIVE`（派生列权威）。切换到权威档前，`/query/cutover-ready`
强制检查：零 PENDING、零 STALE、零未应用事件、规则版本全量对齐、源行版本指针一致、
无运行中的回填。任一不满足，切换请求返回 **409** 并列出阻塞项。切换后若又发生实时
写入，闸门会重新报红。

### 5. 规则版本化与血缘

- 规则存表（`rules`），每次页面调整产生**新版本**，AST 白名单求值
  （只放行比较/算术/布尔/条件表达式，禁止调用、属性、下标、推导式），`and/or`
  严格短路，`x is not None and x > 0` 这类空值守卫天然安全。
- 每次派生写入落一条 `derived_audit`：规则版本、源行版本、**源字段完整快照 JSON**、
  触发原因（`BACKFILL` / `LIVE_CATCHUP`）、所属 run。旧版本审计永不删除，
  任意新值都能回答"由哪版原值计算"。
- 回填运行中换版：run 的 `rule_version_target` 自动抬高，游标回绕按新版本重算。

### 6. 中断与恢复

worker 捕获 SIGTERM 后把 `cursor_id`、统计计数、状态（`INTERRUPTED`）持久化到
`backfill_runs`。同范围再次启动时复用原 run、从断点继续；中断期接入服务的写入照常
入 `change_log`，恢复后在追平阶段消费，不丢事件。

## 快速开始

```bash
# 1. 自动跑完整 8 章端到端演示（会自动 seed、起三个服务、跑完退出断言）
PYTHONPATH=. python3 -P demo.py .

# 2. 或手动启动后打开页面操作
./start_services.sh
# 浏览器打开 http://127.0.0.1:8101
./stop_services.sh

# 3. 单元测试（规则引擎 / 写入路径 / 版本守卫 / 闸门）
PYTHONPATH=. python3 -P -m unittest tests.test_migration -v
```

## 演示八章（`demo.py`，全部走 HTTP 接口，无一次性 SQL）

0. 三进程健康检查，500 行虚构成员全部 PENDING
1. 慢回填 + 实时写入并发：确定性复现版本守卫拦截，旧快照不落库
2. SIGTERM 中断回填：验证断点持久化，中断期写入不丢
3. 同 run 断点恢复：追平积压事件后 500 行全部 COMPUTED
4. 页面调整规则 v2：换版重算，v1 血缘保留；危险表达式被 AST 拒绝
5. 合法空值（COMPUTED+NULL）与未回填（PENDING）查询语义对照
6. 承诺闸门：未对齐时 409，对齐后切换；切换后再写入自动重新报红
7. 血缘核验：最新派生值的规则版本/源行版本与行指针完全一致
8. 查询端三态接口：PENDING/STALE 永远是带 hint 的状态对象而非裸 null

## 目录

```
app/
  db.py              schema、连接、设置项
  rule_engine.py     AST 白名单规则引擎（版本化、短路求值）
  ingest_service.py  接入服务（唯一写入口 + change_log）
  worker.py          回填进程（游标扫描/版本守卫/事件追平/中断恢复）
  query_service.py   查询端（三态、闸门、血缘）
  control_service.py 控制台（页面 + 编排 API + worker 子进程管理）
  simulator.py       实时写入模拟器（含热点 id 确定性竞争）
  http_util.py       标准库 JSON HTTP 框架
  seed.py            500 行虚构样例数据
web/index.html       单页控制台（原生 HTML/CSS/JS）
tests/test_migration.py  单元测试
demo.py              端到端八章演示
```
