"""端到端演示编排：不使用任何一次性 SQL，全部通过三个服务的 HTTP 接口完成交接。

演示章节：
  0. 三进程健康检查，确认初始 500 行全部 PENDING
  1. 启动回填（故意放慢），同时启动实时写入模拟器 → 观察 STALE 与版本守卫拦截
  2. 回填中途中断 → 验证断点持久化，且中断期间写入不丢
  3. 恢复回填 → 从游标断点继续
  4. 页面调整规则（v2）→ 已回填行按新版本重算，血缘保留 v1 记录
  5. 制造规则显式 NULL（合法空值）并与 PENDING 对照
  6. 查询承诺闸门：未满足时 409，满足后切换 DERIVED_AUTHORITATIVE
  7. 血缘核验：指定成员的每个新值都能回答"由哪版原值、哪版规则计算"
  8. 查询端三态对照：PENDING/STALE 不会被渲染成静默 null
"""
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE_CTRL = "http://127.0.0.1:8101"
BASE_INGEST = "http://127.0.0.1:8102"
BASE_QUERY = "http://127.0.0.1:8103"
ROOT = None


def call(url, payload=None, method=None, expect_error=False):
    data = json.dumps(payload).encode() if payload is not None else None
    # 显式 method 优先；否则有 body 才 POST，无 body 默认 GET
    if method is None:
        method = "POST" if data is not None else "GET"
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = json.loads(e.read())
        if expect_error:
            return e.code, body
        raise SystemExit(f"❌ {url} 返回 {e.code}: {body.get('error')}")


def wait_health(url, name):
    for _ in range(50):
        try:
            call(url)
            print(f"  ✅ {name} 就绪：{url}")
            return
        except Exception:
            time.sleep(0.2)
    raise SystemExit(f"❌ {name} 未启动")


def section(title):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def progress_snapshot():
    s = call(BASE_CTRL + "/api/state")
    c = s["status_counts"]
    return (f"PENDING={c['PENDING']:<4} STALE={c['STALE']:<4} "
            f"COMPUTED={c['COMPUTED']:<4} 未应用事件={s['unapplied_events']:<3} "
            f"模拟器累计写入={s['simulator']['writes']}")


def wait_until(predicate, timeout=60, tick=0.8):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = call(BASE_CTRL + "/api/state")
        if predicate(last):
            return last
        time.sleep(tick)
    raise SystemExit(f"❌ 等待条件超时，最后状态: {json.dumps(last, ensure_ascii=False)[:400]}")


def main():
    global ROOT
    ROOT = sys.argv[1] if len(sys.argv) > 1 else "."

    section("第 0 章 · 启动三个独立进程并健康检查")
    subprocess.run(["bash", f"{ROOT}/start_services.sh"], check=True)
    wait_health(BASE_CTRL + "/api/state", "控制台")
    wait_health(BASE_INGEST + "/ingest/health", "接入服务")
    wait_health(BASE_QUERY + "/query/health", "查询端")
    s = call(BASE_CTRL + "/api/state")
    print(f"  初始状态：总 {s['total_members']} 行，{progress_snapshot()}")
    assert s["status_counts"]["PENDING"] == 500, "种子数据应为 500 行 PENDING"

    section("第 1 章 · 慢回填与实时写入并发（版本守卫防旧值覆盖）")
    # 全量范围 1..500 慢回填；热点成员 80/90 位于扫描前段，约 8~9 秒被扫到：
    # 模拟器持续推高它们的 row_version，持快照停顿 60ms 期间再来一次更新，
    # 写回瞬间乐观守卫必然失配 → BLOCKED，旧原值不可能盖掉新原值。
    hot_ids = [80, 90]
    # 随机写入覆盖全表（与全量回填范围一致），热点 id 保证前段确定性竞争
    call(BASE_CTRL + "/api/simulator/start",
         {"id_min": 1, "id_max": 500, "interval": 0.08, "hot_ids": hot_ids})
    run = call(BASE_CTRL + "/api/runs/start",
               {"id_min": 1, "id_max": 500, "batch_delay": 0.02,
                "row_delay": 0.0, "batch_size": 10, "snapshot_gap": 0.06})
    run_id = run["run_id"]
    print(f"  {run['note']}（全量 1..500，批次 10 行、持快照停顿 60ms 模拟计算耗时）")
    print(f"  模拟器同步运行，热点成员 {hot_ids} 每 80ms 被改写一次（确定性竞争）")
    # 让它跑一段时间，观察中间态
    for i in range(6):
        time.sleep(1.2)
        print(f"  t+{(i+1)*1.2:>4.1f}s  {progress_snapshot()}")
    mid = call(BASE_CTRL + "/api/state")
    run_row = next(r for r in mid["runs"] if r["id"] == run_id)
    print(f"  run #{run_id} 已拦截旧快照 {run_row['stale_overwrites_blocked']} 次，"
          f"被拦截的旧快照直接丢弃，事件追平用最新快照重算")
    assert run_row["stale_overwrites_blocked"] > 0, "热点竞争下应出现版本守卫拦截"
    # 抽查热点成员：审计血缘里每条计算记录的源行版本都必须 ≤ 当前 row_version，
    # 且任何旧版本派生值都不可能落库覆盖（拦截计数即证据）
    target = hot_ids[0]
    lin = call(BASE_CTRL + f"/api/lineage/{target}")
    for a in lin["lineage"]:
        assert a["source_row_version_used"] <= lin["member"]["row_version"]
    print(f"  抽查热点成员 #{target}：当前源行 v{lin['member']['row_version']}，"
          f"共 {len(lin['lineage'])} 条计算血缘，全部基于不高于当前的源行版本 ✅")

    section("第 2 章 · 中断回填进程（断点持久化，期间写入继续）")
    call(BASE_CTRL + "/api/simulator/stop", payload={}, method="POST")
    call(BASE_CTRL + f"/api/runs/{run_id}/stop", payload={}, method="POST")
    time.sleep(0.5)
    stopped = call(BASE_CTRL + "/api/runs")
    r = next(x for x in stopped["items"] if x["id"] == run_id)
    print(f"  run #{run_id} 状态={r['status']}，断点 cursor_id={r['cursor_id']}，"
          f"已处理 {r['processed_rows']}/{r['total_rows']}")
    assert r["status"] == "INTERRUPTED"
    before = call(BASE_CTRL + f"/api/members/{target + 1 if target < 500 else target - 1}")
    call(BASE_INGEST + "/ingest/members",
         {"id": before["id"], "fields": {"balance": 424242}})
    after = call(BASE_CTRL + f"/api/members/{before['id']}")
    print(f"  中断期间（模拟器已停）人工修改成员 #{before['id']}：row_version "
          f"v{before['row_version']} → v{after['row_version']}，状态 {after['derived_status']}（事件入队不丢）")
    assert after["row_version"] == before["row_version"] + 1

    section("第 3 章 · 恢复回填（同 run 从断点继续，自动追平中断期积压事件）")
    resumed = call(BASE_CTRL + "/api/runs/start",
                   {"id_min": 1, "id_max": 500, "batch_delay": 0.03})
    assert resumed["run_id"] == run_id, "应复用原 run 而不是新建"
    print(f"  {resumed['note']}")
    wait_until(lambda st: any(x["id"] == run_id and x["status"] == "DONE" for x in st["runs"]),
               timeout=150)
    done = call(BASE_CTRL + "/api/state")
    rd = next(x for x in done["runs"] if x["id"] == run_id)
    print(f"  run #{run_id} 完成：处理 {rd['processed_rows']} 行，写入 {rd['computed_writes']}，"
          f"守卫拦截 {rd['stale_overwrites_blocked']}")
    settled = call(BASE_CTRL + "/api/state")
    print(f"  积压事件全部追平后：{progress_snapshot()}")
    assert settled["status_counts"]["COMPUTED"] == 500
    assert settled["unapplied_events"] == 0 and settled["status_counts"]["STALE"] == 0

    section("第 4 章 · 页面调整规则 v2：换版重算，v1 血缘保留")
    v2 = (
        "'VIP' if credit_score is not None and credit_score >= 720 and balance >= 200000 "
        "and is_active == 1 else 'DORMANT' if is_active == 0 or active_days < 10 "
        "else 'GROWTH' if credit_score is not None and credit_score >= 600 and age >= 18 "
        "else 'BASIC'"
    )
    rule = call(BASE_CTRL + "/api/rules",
                {"name": "客户分层-v2", "expr": v2,
                 "description": "演示中调整：新增沉睡分层", "activate": True})
    print(f"  新规则已创建并激活：v{rule['version']}（AST 白名单校验通过）")
    bad = call(BASE_CTRL + "/api/rules",
               {"name": "恶意规则", "expr": "__import__('os').system('id')",
                "activate": False}, expect_error=True)
    print(f"  危险表达式被拒绝：HTTP {bad[0]} — {bad[1]['error'][:60]}")
    run2 = call(BASE_CTRL + "/api/runs/start",
                {"id_min": 1, "id_max": 500, "batch_delay": 0.03})
    wait_until(lambda st: any(x["id"] == run2["run_id"] and x["status"] == "DONE"
                              for x in st["runs"]), timeout=150)
    s2 = call(BASE_CTRL + "/api/state")
    print(f"  v2 重算完成：{progress_snapshot()}；目标规则版本 "
          f"v{next(x for x in s2['runs'] if x['id']==run2['run_id'])['rule_version_target']}")
    lin2 = call(BASE_CTRL + "/api/lineage/1")
    versions_used = sorted({a["rule_version_at_compute"] for a in lin2["lineage"]})
    print(f"  成员 #1 血缘中保留的规则版本：{versions_used}（v1 记录未被删除）")
    assert versions_used == [1, 2]

    section("第 5 章 · 合法空值 vs 未回填（查询端语义对照）")
    v3 = ("None if credit_score is None else 'VIP' if credit_score >= 720 "
          "and balance >= 200000 else 'GROWTH' if credit_score >= 600 else 'BASIC'")
    call(BASE_CTRL + "/api/rules",
         {"name": "客户分层-v3-含空值", "expr": v3,
          "description": "信用分缺失显式产出 None", "activate": True})
    run3 = call(BASE_CTRL + "/api/runs/start",
                {"id_min": 1, "id_max": 500, "batch_delay": 0.03})
    wait_until(lambda st: any(x["id"] == run3["run_id"] and x["status"] == "DONE"
                              for x in st["runs"]), timeout=150)
    s3 = call(BASE_CTRL + "/api/state")
    print(f"  v3 重算完成，COMPUTED 中的合法空值行数：{s3['legal_null_computed']}")
    assert s3["legal_null_computed"] > 0
    # 新插入一行不回填，制造 PENDING 对照
    call(BASE_INGEST + "/ingest/members",
         {"id": 999001, "fields": {"name": "测试-未回填", "age": 30, "credit_score": 700,
                                   "balance": 1000, "active_days": 200, "is_active": 1,
                                   "city_tier": "TIER1"}})
    q_pending = call(BASE_QUERY + "/query/members/999001")
    null_member = next(m for m in call(BASE_QUERY +
                      "/query/members?limit=200&status=COMPUTED")["items"]
                      if m["derived"]["is_legal_null"])
    q_null = call(BASE_QUERY + f"/query/members/{null_member['id']}")
    print(f"  未回填行 #{q_pending['id']} 查询返回：status={q_pending['derived']['status']}，"
          f"is_legal_null={q_pending['derived']['is_legal_null']}，hint={q_pending['derived']['hint']}")
    print(f"  合法空值行 #{q_null['id']} 查询返回：status={q_null['derived']['status']}，"
          f"value={q_null['derived']['value']}，is_legal_null={q_null['derived']['is_legal_null']}")
    assert q_pending["derived"]["status"] == "PENDING"
    assert q_pending["derived"]["is_legal_null"] is False
    assert q_null["derived"]["status"] == "COMPUTED"
    assert q_null["derived"]["value"] is None and q_null["derived"]["is_legal_null"] is True

    section("第 6 章 · 查询承诺切换闸门（不满足 → 409，满足 → 切换）")
    blocked = call(BASE_CTRL + "/api/promise",
                   {"value": "DERIVED_AUTHORITATIVE"}, expect_error=True)
    print(f"  存在 PENDING 时强行切换：HTTP {blocked[0]} — {blocked[1]['error'][:80]}")
    assert blocked[0] == 409
    # 回填新增行后闸门才放行
    run4 = call(BASE_CTRL + "/api/runs/start",
                {"id_min": 1, "id_max": 999001, "batch_delay": 0.03})
    wait_until(lambda st: any(x["id"] == run4["run_id"] and x["status"] == "DONE"
                              for x in st["runs"]), timeout=150)
    ready = call(BASE_CTRL + "/api/cutover-ready")
    print(f"  全量对齐后闸门检查：ready={ready['ready']}，counts={ready['counts']}")
    assert ready["ready"]
    switched = call(BASE_CTRL + "/api/promise",
                    {"value": "DERIVED_AUTHORITATIVE"})
    print(f"  查询承诺已切换：{switched['query_promise']}（此刻起查询端可直接信任派生列）")
    # 切换后又来实时写入 → 闸门重新报红（承诺需要重新追平才能维持）
    call(BASE_INGEST + "/ingest/members", {"id": 1, "fields": {"balance": 1}})
    time.sleep(1)
    regate = call(BASE_CTRL + "/api/cutover-ready")
    print(f"  切换后再发生实时写入，闸门重新报红：blockers={regate['blockers']}")
    assert not regate["ready"]
    run5 = call(BASE_CTRL + "/api/runs/start",
                {"id_min": 1, "id_max": 999001, "batch_delay": 0.03})
    wait_until(lambda st: any(x["id"] == run5["run_id"] and x["status"] == "DONE"
                              for x in st["runs"]), timeout=150)
    assert call(BASE_CTRL + "/api/cutover-ready")["ready"]

    section("第 7 章 · 血缘核验：每个新值都能回答由哪版原值计算")
    lin = call(BASE_CTRL + "/api/lineage/1")
    print(f"  成员 #1 当前 row_version=v{lin['member']['row_version']}，共 {len(lin['lineage'])} 条计算血缘：")
    for a in lin["lineage"][-4:]:
        print(f"    audit#{a['audit_seq']:<3} [{a['reason']:<11}] 规则v{a['rule_version_at_compute']}"
              f" × 源行v{a['source_row_version_used']} → {a['computed_value']}")
    latest = lin["lineage"][-1]
    assert latest["source_row_version_used"] == lin["member"]["row_version"]
    assert latest["rule_version_at_compute"] == lin["member"]["derived_rule_version"]
    print("  最新一条血缘的源行版本/规则版本与行内指针完全一致 ✅")

    section("第 8 章 · 查询端三态接口对照（迁移期承诺下绝不返回裸 null）")
    call(BASE_CTRL + "/api/promise", {"value": "DUAL_WRITE_PENDING"})
    call(BASE_INGEST + "/ingest/members", {"id": 999002, "fields": {"name": "三态对照"}})
    call(BASE_INGEST + "/ingest/members", {"id": 999001, "fields": {"balance": 2}})
    time.sleep(1.5)
    sample = call(BASE_QUERY + "/query/members?limit=3")
    print(f"  查询端返回 query_promise={sample['query_promise']}，派生字段均为带状态对象：")
    for item in sample["items"][:2]:
        print(f"    #{item['id']}: {json.dumps(item['derived'], ensure_ascii=False)}")
    pending_view = call(BASE_QUERY + "/query/members/999002")["derived"]
    stale_view = call(BASE_QUERY + "/query/members/999001")["derived"]
    print(f"  PENDING 视图：{json.dumps(pending_view, ensure_ascii=False)}")
    print(f"  STALE 视图：  {json.dumps(stale_view, ensure_ascii=False)}")
    assert pending_view["status"] == "PENDING" and pending_view["is_legal_null"] is False
    assert stale_view["status"] == "STALE" and "hint" in stale_view

    section("🎉 全部章节通过")
    print("  服务仍在运行：控制台 http://127.0.0.1:8101 （可手动调整规则、回填、中断、查血缘）")
    print("  停止服务：./stop_services.sh；worker 日志：data/worker_*.log")


if __name__ == "__main__":
    main()
