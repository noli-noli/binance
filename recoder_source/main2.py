import argparse
import asyncio
import json
import os
import time
from collections import deque
from datetime import datetime, timezone

import requests
import websockets

SYMBOL = "BTCUSDT"
SNAPSHOT_LIMIT = 5000

WS_BASE_URL = "wss://data-stream.binance.vision/ws"
REST_URL = "https://data-api.binance.vision/api/v3/depth"

WS_PING_INTERVAL_SEC = 20
WS_PING_TIMEOUT_SEC = 20
WS_BUFFER_WAIT_SEC = 2
WS_RECV_TIMEOUT_SEC = 5
WS_MAX_SESSION_SEC = 23 * 60 * 60 + 55 * 60
RECONNECT_BASE_DELAY_SEC = 1
RECONNECT_MAX_DELAY_SEC = 30
LOG_INTERVAL_SEC = 10
OUTPUT_DIR = os.path.dirname(__file__)


def current_unix_time():
    return time.time()


def format_unix_time(ts):
    if ts is None:
        return "None"
    return f"{ts:.6f}"


def build_ws_url(symbol: str):
    return f"{WS_BASE_URL}/{symbol.lower()}@depth@100ms"


def unix_time_to_date_str(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def get_log_file_path(ts):
    return os.path.join(OUTPUT_DIR, f"main2_log_{unix_time_to_date_str(ts)}.txt")


def get_depth_diff_file_path(ts):
    return os.path.join(OUTPUT_DIR, f"depth_diff_{unix_time_to_date_str(ts)}.jsonl")


def get_initial_orderbook_file_path(ts):
    return os.path.join(OUTPUT_DIR, f"initial_orderbook_{unix_time_to_date_str(ts)}.jsonl")


def get_final_orderbook_file_path(ts):
    return os.path.join(OUTPUT_DIR, f"final_orderbook_{unix_time_to_date_str(ts)}.jsonl")


def append_line(file_path: str, line: str):
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def append_log_line(state: dict, message: str, ts=None):
    if ts is None:
        ts = current_unix_time()
    append_line(get_log_file_path(ts), message)


def append_diff_event(event: dict):
    append_line(
        get_depth_diff_file_path(event["timestamp"]),
        json.dumps(event, ensure_ascii=False),
    )


def append_initial_orderbook(book: dict):
    ts = book.get("lastUpdateTime", current_unix_time())
    append_line(
        get_initial_orderbook_file_path(ts),
        json.dumps(book, ensure_ascii=False),
    )


def append_final_orderbook(book: dict):
    ts = book.get("lastUpdateTime", current_unix_time())
    append_line(
        get_final_orderbook_file_path(ts),
        json.dumps(book, ensure_ascii=False),
    )


async def periodic_status_logger(state: dict, stop_event: asyncio.Event):
    while not stop_event.is_set():
        await asyncio.sleep(LOG_INTERVAL_SEC)

        line = (
            f"log_time={format_unix_time(current_unix_time())}, "
            f"start_time={format_unix_time(state['start_time'])}, "
            f"last_fetch_time={format_unix_time(state['last_fetch_time'])}, "
            f"ws_message_count={state['ws_message_count']}, "
            f"error_count={state['error_count']}"
        )

        if state["last_error"] is not None:
            line += f", last_error={state['last_error']}"

        append_log_line(state, line)


async def stdin_stop_watcher(stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            line = await asyncio.to_thread(input)
        except EOFError:
            return

        if line.strip().lower() == "stop":
            stop_event.set()
            return


# RESTFULL API経由によるスナップショットの獲得
def fetch_snapshot(symbol: str, limit: int = 5000, state=None):
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

    if state is not None:
        state["last_fetch_time"] = snapshot_time

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


def ensure_worker_running(worker: asyncio.Task):
    if worker.done():
        exc = worker.exception()
        if exc is None:
            raise RuntimeError("WS受信workerが停止")
        raise exc


async def ws_buffer_worker(ws, buffer: deque, stop_event: asyncio.Event, state: dict):
    while not stop_event.is_set():
        try:
            msg = await asyncio.wait_for(ws.recv(), timeout=WS_RECV_TIMEOUT_SEC)
            event = json.loads(msg)
            event["timestamp"] = current_unix_time()

            buffer.append(event)
            state["ws_message_count"] += 1
            append_diff_event(event)

        except asyncio.TimeoutError:
            continue
        except websockets.ConnectionClosed as e:
            raise ConnectionError(f"WS切断: code={e.code}, reason={e.reason}") from e


async def maintain_local_order_book_once(symbol="BTCUSDT", run_deadline=None, snapshot_limit=5000, state=None, shutdown_event=None):
    buffer = deque()
    stop_event = asyncio.Event()
    session_start = time.monotonic()
    session_end_time = session_start + WS_MAX_SESSION_SEC
    if run_deadline is not None:
        session_end_time = min(session_end_time, run_deadline)

    async with websockets.connect(
        build_ws_url(symbol),
        ping_interval=WS_PING_INTERVAL_SEC,
        ping_timeout=WS_PING_TIMEOUT_SEC,
        close_timeout=5,
        max_queue=None,
    ) as ws:
        worker = asyncio.create_task(ws_buffer_worker(ws, buffer, stop_event, state))

        try:
            # バッファ確保
            start_wait = time.monotonic()
            while len(buffer) < 5 and time.monotonic() - start_wait < WS_BUFFER_WAIT_SEC:
                ensure_worker_running(worker)
                if shutdown_event is not None and shutdown_event.is_set():
                    return None, False
                await asyncio.sleep(0.05)

            # snapshot
            book = fetch_snapshot(symbol, snapshot_limit, state=state)
            append_initial_orderbook(book)
            last_id = book["lastUpdateId"]

            await asyncio.sleep(0.1)

            # 古いイベント削除
            while buffer and int(buffer[0]["u"]) <= last_id:
                buffer.popleft()

            # 初期同期
            synced = False
            while True:
                ensure_worker_running(worker)

                if shutdown_event is not None and shutdown_event.is_set():
                    return book, False

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
            while time.monotonic() < session_end_time:
                ensure_worker_running(worker)

                if shutdown_event is not None and shutdown_event.is_set():
                    return book, False

                if not buffer:
                    await asyncio.sleep(0.01)
                    continue

                e = buffer.popleft()

                if int(e["u"]) <= book["lastUpdateId"]:
                    continue

                if int(e["U"]) > book["lastUpdateId"] + 1:
                    raise RuntimeError("再同期必要")

                apply_event(book, e)

            completed = run_deadline is not None and session_end_time >= run_deadline
            return book, completed

        finally:
            stop_event.set()
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass


async def maintain_local_order_book(symbol="BTCUSDT", duration_sec=None, snapshot_limit=5000, shutdown_event=None):
    book = None
    run_deadline = None
    reconnect_count = 0
    state = {
        "start_time": current_unix_time(),
        "last_fetch_time": None,
        "ws_message_count": 0,
        "error_count": 0,
        "last_error": None,
    }
    log_stop_event = asyncio.Event()
    logger_task = asyncio.create_task(periodic_status_logger(state, log_stop_event))

    append_log_line(
        state,
        f"start_time={format_unix_time(state['start_time'])}",
        ts=state["start_time"],
    )

    if duration_sec is not None:
        run_deadline = time.monotonic() + duration_sec

    try:
        while True:
            if shutdown_event is not None and shutdown_event.is_set():
                break

            if run_deadline is not None and time.monotonic() >= run_deadline:
                break

            try:
                book, completed = await maintain_local_order_book_once(
                    symbol=symbol,
                    run_deadline=run_deadline,
                    snapshot_limit=snapshot_limit,
                    state=state,
                    shutdown_event=shutdown_event,
                )
                reconnect_count = 0

                if shutdown_event is not None and shutdown_event.is_set():
                    break

                if completed:
                    break

                print("24時間上限を考慮してWSを再接続します")

            except (
                ConnectionError,
                OSError,
                requests.RequestException,
                RuntimeError,
                json.JSONDecodeError,
            ) as e:
                reconnect_count += 1
                delay = min(RECONNECT_BASE_DELAY_SEC * (2 ** (reconnect_count - 1)), RECONNECT_MAX_DELAY_SEC)
                state["error_count"] += 1
                state["last_error"] = str(e)
                print(f"接続例外発生: {e}")

                if run_deadline is not None and time.monotonic() >= run_deadline:
                    break

                if shutdown_event is not None and shutdown_event.is_set():
                    break

                print(f"{delay}秒後に再接続します")
                await asyncio.sleep(delay)

        if book is None:
            raise RuntimeError("収集完了前に有効な板情報を構築できませんでした")

        return book
    finally:
        log_stop_event.set()
        logger_task.cancel()
        try:
            await logger_task
        except asyncio.CancelledError:
            pass

        append_log_line(
            state,
            (
                f"log_time={format_unix_time(current_unix_time())}, "
                f"start_time={format_unix_time(state['start_time'])}, "
                f"last_fetch_time={format_unix_time(state['last_fetch_time'])}, "
                f"ws_message_count={state['ws_message_count']}, "
                f"error_count={state['error_count']}, "
                f"last_error={state['last_error']}"
            ),
        )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default=SYMBOL)
    parser.add_argument("--snapshot-limit", type=int, default=SNAPSHOT_LIMIT)

    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--stdin-stop", action="store_true")
    mode_group.add_argument("--duration-sec", type=int)

    return parser.parse_args()


async def main():
    args = parse_args()
    shutdown_event = asyncio.Event()
    stdin_task = None

    if args.duration_sec is not None and args.duration_sec <= 0:
        raise ValueError("duration-secは1以上を指定してください")

    if args.stdin_stop:
        print("標準入力で stop と入力すると終了します")
        stdin_task = asyncio.create_task(stdin_stop_watcher(shutdown_event))

    try:
        book = await maintain_local_order_book(
            symbol=args.symbol,
            duration_sec=args.duration_sec,
            snapshot_limit=args.snapshot_limit,
            shutdown_event=shutdown_event,
        )
        return book
    finally:
        shutdown_event.set()
        if stdin_task is not None:
            stdin_task.cancel()
            try:
                await stdin_task
            except asyncio.CancelledError:
                pass


book = asyncio.run(main())


# =========================
# JSON保存
# =========================

append_final_orderbook(book)

print("保存完了:")
print(" - initial_orderbook_YYYY-MM-DD.jsonl（初期板を日付別保存）")
print(" - depth_diff_YYYY-MM-DD.jsonl（差分を日付別保存）")
print(" - final_orderbook_YYYY-MM-DD.jsonl（最終板を日付別保存）")
print(" - main2_log_YYYY-MM-DD.txt（ログを日付別保存）")
