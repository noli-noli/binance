import asyncio
import json
import time
from collections import deque

import requests
import websockets

SYMBOL = "BTCUSDT"
DURATION_SEC = 10
SNAPSHOT_LIMIT = 5000

WS_URL = f"wss://data-stream.binance.vision/ws/{SYMBOL.lower()}@depth@100ms"
REST_URL = "https://data-api.binance.vision/api/v3/depth"


def current_unix_time():
    return time.time()

# RESTFULL API経由によるスナップショットの獲得
def fetch_snapshot(symbol: str, limit: int = 5000):
    snapshot_time = current_unix_time()
    res = requests.get(
        REST_URL,
        params={"symbol": symbol.upper(), "limit": limit},
        timeout=10,
    )
    if not res.ok:
        raise RuntimeError(f"snapshot取得失敗: status={res.status_code}, body={res.text}")

    data = res.json()
    if "lastUpdateId" not in data or "bids" not in data or "asks" not in data:
        raise RuntimeError(f"snapshot形式不正: {data}")

    return {
        "lastUpdateId": int(data["lastUpdateId"]),
        "lastUpdateTime": snapshot_time,
        "bids": {float(p): float(q) for p, q in data["bids"]},
        "asks": {float(p): float(q) for p, q in data["asks"]},
    }


def apply_event(book: dict, event: dict):
    for p_str, q_str in event["b"]:
        p = float(p_str)
        q = float(q_str)
        if q == 0.0:
            book["bids"].pop(p, None)
        else:
            book["bids"][p] = q

    for p_str, q_str in event["a"]:
        p = float(p_str)
        q = float(q_str)
        if q == 0.0:
            book["asks"].pop(p, None)
        else:
            book["asks"][p] = q

    book["lastUpdateId"] = int(event["u"])
    book["lastUpdateTime"] = event["timestamp"]


async def ws_buffer_worker(ws, buffer: deque, stop_event: asyncio.Event, events: list):
    while not stop_event.is_set():
        try:
            msg = await asyncio.wait_for(ws.recv(), timeout=5)
            event = json.loads(msg)
            event["timestamp"] = current_unix_time()

            buffer.append(event)
            events.append(event)   # ★ ここで全差分を保存

        except asyncio.TimeoutError:
            continue


async def maintain_local_order_book(symbol="BTCUSDT", duration_sec=10, snapshot_limit=5000):
    buffer = deque()
    stop_event = asyncio.Event()
    events = []   # ★ 保存用

    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
        worker = asyncio.create_task(ws_buffer_worker(ws, buffer, stop_event, events))

        try:
            # バッファ確保
            start_wait = time.monotonic()
            while len(buffer) < 5 and time.monotonic() - start_wait < 2:
                await asyncio.sleep(0.05)

            # snapshot
            book = fetch_snapshot(symbol, snapshot_limit)
            last_id = book["lastUpdateId"]

            await asyncio.sleep(0.1)

            # 古いイベント削除
            while buffer and int(buffer[0]["u"]) <= last_id:
                buffer.popleft()

            # 初期同期
            synced = False
            while True:
                while buffer:
                    e = buffer[0]
                    U = int(e["U"])
                    u = int(e["u"])

                    if U <= last_id + 1 <= u:
                        synced = True
                        break

                    buffer.popleft()

                if synced:
                    break

                await asyncio.sleep(0.05)

                if len(buffer) > 2000:
                    raise RuntimeError("初期同期失敗")

            # 適用
            while buffer:
                e = buffer.popleft()

                if int(e["u"]) <= book["lastUpdateId"]:
                    continue

                if int(e["U"]) > book["lastUpdateId"] + 1:
                    raise RuntimeError("同期ずれ")

                apply_event(book, e)

            # 指定時間維持
            end_time = time.monotonic() + duration_sec

            while time.monotonic() < end_time:
                if not buffer:
                    await asyncio.sleep(0.01)
                    continue

                e = buffer.popleft()

                if int(e["u"]) <= book["lastUpdateId"]:
                    continue

                if int(e["U"]) > book["lastUpdateId"] + 1:
                    raise RuntimeError("再同期必要")

                apply_event(book, e)

            return book, events   # ★ eventsも返す

        finally:
            stop_event.set()
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass

async def main():
    # 実行
    book, events = await maintain_local_order_book(
        symbol="BTCUSDT",
        duration_sec=60,
        snapshot_limit=5000,
    )
    return book, events

book, events = asyncio.run(main())


# =========================
# JSON保存
# =========================

# 差分イベント保存
with open("depth_diff.json", "w", encoding="utf-8") as f:
    json.dump(events, f, ensure_ascii=False)

# 最終板も保存
with open("final_orderbook.json", "w", encoding="utf-8") as f:
    json.dump(book, f, ensure_ascii=False, indent=2)

print("保存完了:")
print(" - depth_diff.json（全差分）")
print(" - final_orderbook.json（最終板）")
