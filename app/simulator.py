"""实时写入模拟器：在独立线程中周期性调用接入服务修改成员。

用于演示"回填进行中原记录仍被实时更新"：每次更新都经过接入服务的正式写入路径
（row_version 推进 + STALE 置位 + change_log 事件），回填进程必须靠版本守卫
放弃旧快照，再由事件追平把新快照算出来。
"""
import random
import threading
import time
import urllib.request
import json


class IngestSimulator:
    def __init__(self, ingest_url: str = "http://127.0.0.1:8102",
                 id_min: int = 1, id_max: int = 500, interval: float = 0.25):
        self.ingest_url = ingest_url
        self.id_min = id_min
        self.id_max = id_max
        self.interval = interval
        self._thread = None
        self._stop = threading.Event()
        self.writes = 0
        self.last_target = None
        self.lock = threading.Lock()
        # 定向热点：每轮额外对这些 id 写入，确定性制造版本守卫竞争
        self.hot_ids: list[int] = []

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _post(self, path: str, payload: dict):
        req = urllib.request.Request(
            self.ingest_url + path,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())

    def _loop(self):
        rng = random.Random()
        while not self._stop.is_set():
            member_id = rng.randint(self.id_min, self.id_max)
            field = rng.choice(("credit_score", "balance", "active_days", "is_active"))
            if field == "credit_score":
                value = rng.randint(450, 850)
            elif field == "balance":
                value = rng.randint(0, 900000)
            elif field == "active_days":
                value = rng.randint(0, 365)
            else:
                value = rng.choice([0, 1])
            try:
                self._post(f"/ingest/members", {"id": member_id, "fields": {field: value}})
                with self.lock:
                    self.writes += 1
                    self.last_target = member_id
                # 热点成员每轮都被再写一次，与回填扫描形成稳定竞争
                for hot in self.hot_ids:
                    self._post("/ingest/members",
                               {"id": hot, "fields": {"balance": rng.randint(0, 900000)}})
                    with self.lock:
                        self.writes += 1
                        self.last_target = hot
            except Exception as e:
                print(f"[simulator] 写入失败: {e}", flush=True)
            self._stop.wait(self.interval)

    def start(self):
        if self.running:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._thread = None

    def status(self) -> dict:
        with self.lock:
            return {"running": self.running, "writes": self.writes,
                    "last_target": self.last_target, "hot_ids": list(self.hot_ids),
                    "interval": self.interval, "range": [self.id_min, self.id_max]}
