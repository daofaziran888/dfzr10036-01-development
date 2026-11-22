"""经前端 Vite 代理（5173）走完整剧本，结束后重置回题设初始场景。"""
from __future__ import annotations

import json
import urllib.request

BASE = "http://127.0.0.1:5173/api/backfill-coverage"


def call(method: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def main() -> None:
    call("POST", "/reset")
    d1 = call("POST", "/dispatch", {
        "stream": "order_events_cdc", "lo": 101, "hi": 130,
        "executor": "新执行端", "batch_no": "BF-NEW-0001",
    })
    call("POST", "/connection", {"batch_no": "BF-OLD-0001", "connected": True})
    old = call("POST", "/writes", {
        "stream": "order_events_cdc", "batch_no": "BF-OLD-0001",
        "positions": [115], "executor": "旧执行端",
    })
    new = call("POST", "/writes", {
        "stream": "order_events_cdc", "batch_no": "BF-NEW-0001",
        "positions": [121, 122, 123, 124], "executor": "新执行端",
    })
    intr = call("POST", "/interrupt", {"stream": "order_events_cdc", "batch_no": "BF-NEW-0001"})
    target = call("GET", "/target-rows/order_events_cdc")
    d2 = call("POST", "/dispatch", {
        "stream": "order_events_cdc", "lo": 101, "hi": 130,
        "executor": "第二轮执行端", "batch_no": "BF-NEW-0002",
    })
    print("经前端代理 E2E:")
    print(" 派发1受理:", d1["dispatch"]["accepted_ranges"])
    print(" 旧端115:", old["accepted"], "| 新端121-124:", new["accepted"],
          "| 中断释放:", intr["released_ranges"])
    r115 = [r for r in target["items"] if r["pos"] == 115]
    print(" 115 唯一行:", r115, "| 目标表总行数:", target["total"])
    print(" 派发2受理:", d2["dispatch"]["accepted_ranges"])
    print(" 仍占用:", d2["view"]["occupied_ranges"],
          "| 可派发:", d2["view"]["dispatchable_ranges"])
    print(" 重置:", call("POST", "/reset")["message"])
    init = call("GET", "/view/order_events_cdc")
    print(" 初始视图: 已覆盖", init["covered_ranges"],
          "占用", init["occupied_ranges"],
          "待派发", init["dispatchable_ranges"])


if __name__ == "__main__":
    main()
