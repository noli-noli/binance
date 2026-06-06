#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from collections import deque
from dataclasses import dataclass, field
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
WS_HANDOFF_LEAD_SEC = 10 * 60
WS_HANDOFF_SYNC_TIMEOUT_SEC = 60
WS_MAX_BUFFER_EVENTS = 100_000
RECONNECT_BASE_DELAY_SEC = 1
RECONNECT_MAX_DELAY_SEC = 30
LOG_INTERVAL_SEC = 10
OUTPUT_DIR = os.path.dirname(__file__)


class SyncGapError(RuntimeError):
    pass


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
    return os.path.join(OUTPUT_DIR, f"main3_log_{unix_time_to_date_str(ts)}.txt")


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


def event_first_id(event: dict) -> int:
    return int(event["U"])


def event_final_id(event: dict) -> int:
    return int(event["u"])


def is_depth_event(event: dict) -> bool:
    return all(key in event for key in ("U", "u", "b", "a"))


def fetch_snapshot(symbol: str, limit: int = 5000, state=None):
    snapshot_time = current_unix_time()
    res = requests.get(
        REST_URL,
        params={"symbol": symbol.upper(), "limit": limit},
        timeout=10,
    )
    if not res.ok:
        raise RuntimeError(f"snapshot fetch failed: status={res.status_code}, body={res.text}")

    data = res.json()
    if "lastUpdateId" not in data or "bids" not in data or "asks" not in data:
        raise RuntimeError(f"invalid snapshot format: {data}")

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


def apply_and_record_event(book: dict, event: dict, state: dict, source_name: str):
    last_id = int(book["lastUpdateId"])
    first_id = event_first_id(event)
    final_id = event_final_id(event)

    if final_id <= last_id:
        state["skipped_event_count"] += 1
        return False

    expected_id = last_id + 1
    if first_id > expected_id:
        raise SyncGapError(
            f"missed depth events on {source_name}: expected={expected_id}, "
            f"next_U={first_id}, next_u={final_id}"
        )

    append_diff_event(event)
    apply_event(book, event)
    state["applied_event_count"] += 1
    state["last_update_id"] = int(book["lastUpdateId"])
    state["last_update_time"] = float(book["lastUpdateTime"])
    return True


@dataclass
class DepthStreamSession:
    name: str
    symbol: str
    state: dict
    buffer: deque = field(default_factory=deque)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    ws: object | None = None
    worker: asyncio.Task | None = None
    connected_at_mono: float | None = None
    connected_at_unix: float | None = None
    server_shutdown_received: bool = False

    async def start(self):
        self.connected_at_mono = time.monotonic()
        self.connected_at_unix = current_unix_time()
        self.ws = await websockets.connect(
            build_ws_url(self.symbol),
            ping_interval=WS_PING_INTERVAL_SEC,
            ping_timeout=WS_PING_TIMEOUT_SEC,
            close_timeout=5,
            max_queue=None,
        )
        self.worker = asyncio.create_task(ws_buffer_worker(self))
        append_log_line(
            self.state,
            f"session_start name={self.name}, time={format_unix_time(self.connected_at_unix)}",
            ts=self.connected_at_unix,
        )

    async def close(self):
        self.stop_event.set()
        if self.worker is not None:
            self.worker.cancel()
            try:
                await self.worker
            except asyncio.CancelledError:
                pass
        if self.ws is not None:
            try:
                await self.ws.close()
            except Exception:
                pass
        append_log_line(
            self.state,
            f"session_close name={self.name}, buffer={len(self.buffer)}",
        )

    def age_sec(self):
        if self.connected_at_mono is None:
            return 0.0
        return time.monotonic() - self.connected_at_mono

    def ensure_running(self):
        if self.worker is None:
            raise RuntimeError(f"{self.name}: WS worker is not started")
        if self.worker.done():
            if self.worker.cancelled():
                raise ConnectionError(f"{self.name}: WS worker was cancelled")
            exc = self.worker.exception()
            if exc is None:
                raise ConnectionError(f"{self.name}: WS worker stopped")
            raise exc


async def ws_buffer_worker(session: DepthStreamSession):
    assert session.ws is not None
    while not session.stop_event.is_set():
        try:
            msg = await asyncio.wait_for(session.ws.recv(), timeout=WS_RECV_TIMEOUT_SEC)
            event = json.loads(msg)

            if event.get("e") == "serverShutdown":
                session.server_shutdown_received = True
                session.state["server_shutdown_count"] += 1
                append_log_line(
                    session.state,
                    f"server_shutdown name={session.name}, event={event}",
                )
                continue

            if not is_depth_event(event):
                session.state["ignored_message_count"] += 1
                append_log_line(
                    session.state,
                    f"ignored_non_depth_message name={session.name}, message={event}",
                )
                continue

            event["timestamp"] = current_unix_time()
            session.buffer.append(event)
            session.state["ws_message_count"] += 1

            if len(session.buffer) > WS_MAX_BUFFER_EVENTS:
                raise RuntimeError(f"{session.name}: WS buffer overflow: {len(session.buffer)}")

        except asyncio.TimeoutError:
            continue
        except websockets.ConnectionClosed as e:
            raise ConnectionError(f"{session.name}: WS closed: code={e.code}, reason={e.reason}") from e


async def periodic_status_logger(state: dict, stop_event: asyncio.Event):
    while not stop_event.is_set():
        await asyncio.sleep(LOG_INTERVAL_SEC)

        line = (
            f"log_time={format_unix_time(current_unix_time())}, "
            f"start_time={format_unix_time(state['start_time'])}, "
            f"last_fetch_time={format_unix_time(state['last_fetch_time'])}, "
            f"ws_message_count={state['ws_message_count']}, "
            f"applied_event_count={state['applied_event_count']}, "
            f"skipped_event_count={state['skipped_event_count']}, "
            f"handoff_count={state['handoff_count']}, "
            f"error_count={state['error_count']}, "
            f"last_update_id={state['last_update_id']}"
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


async def wait_for_buffer(session: DepthStreamSession, min_count: int, timeout_sec: float, shutdown_event=None):
    deadline = time.monotonic() + timeout_sec
    while len(session.buffer) < min_count:
        session.ensure_running()
        if shutdown_event is not None and shutdown_event.is_set():
            return False
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(0.05)
    return True


async def wait_for_syncable_snapshot(
    session: DepthStreamSession,
    symbol: str,
    snapshot_limit: int,
    state: dict,
    shutdown_event=None,
    timeout_sec=WS_HANDOFF_SYNC_TIMEOUT_SEC,
):
    if not await wait_for_buffer(session, 5, WS_BUFFER_WAIT_SEC, shutdown_event):
        raise RuntimeError(f"{session.name}: no initial depth events")

    while True:
        session.ensure_running()
        if shutdown_event is not None and shutdown_event.is_set():
            raise asyncio.CancelledError()
        break

    first_buffered_u = event_first_id(session.buffer[0])
    snapshot = fetch_snapshot(symbol, snapshot_limit, state=state)
    snapshot_id = int(snapshot["lastUpdateId"])

    if snapshot_id < first_buffered_u:
        append_log_line(
            state,
            f"snapshot_too_old name={session.name}, snapshot_id={snapshot_id}, first_U={first_buffered_u}",
        )
        raise RuntimeError(
            f"{session.name}: snapshot is older than buffered stream "
            f"(snapshot_id={snapshot_id}, first_U={first_buffered_u})"
        )

    while session.buffer and event_final_id(session.buffer[0]) <= snapshot_id:
        session.buffer.popleft()

    deadline = time.monotonic() + timeout_sec
    while not session.buffer:
        session.ensure_running()
        if shutdown_event is not None and shutdown_event.is_set():
            raise asyncio.CancelledError()
        if time.monotonic() >= deadline:
            raise RuntimeError(f"{session.name}: no post-snapshot depth events")
        await asyncio.sleep(0.05)

    first_event = session.buffer[0]
    expected_id = snapshot_id + 1
    if event_first_id(first_event) <= expected_id <= event_final_id(first_event):
        append_initial_orderbook(snapshot)
        append_log_line(
            state,
            f"snapshot_synced name={session.name}, snapshot_id={snapshot_id}, "
            f"first_U={event_first_id(first_event)}, first_u={event_final_id(first_event)}",
        )
        return snapshot

    append_log_line(
        state,
        f"snapshot_not_bridgeable name={session.name}, snapshot_id={snapshot_id}, "
        f"first_U={event_first_id(first_event)}, first_u={event_final_id(first_event)}",
    )
    raise RuntimeError(
        f"{session.name}: snapshot cannot be bridged "
        f"(snapshot_id={snapshot_id}, first_U={event_first_id(first_event)}, first_u={event_final_id(first_event)})"
    )


async def open_initial_session(symbol: str, snapshot_limit: int, state: dict, shutdown_event=None):
    session = DepthStreamSession("active-0", symbol, state)
    await session.start()
    try:
        book = await wait_for_syncable_snapshot(session, symbol, snapshot_limit, state, shutdown_event)
        while session.buffer:
            event = session.buffer.popleft()
            apply_and_record_event(book, event, state, session.name)
        return session, book
    except Exception:
        await session.close()
        raise


def should_start_handoff(session: DepthStreamSession, max_session_sec: float, handoff_lead_sec: float):
    if session.server_shutdown_received:
        return True
    return session.age_sec() >= max(0.0, max_session_sec - handoff_lead_sec)


def replacement_bridge_status(replacement: DepthStreamSession, book: dict):
    expected_id = int(book["lastUpdateId"]) + 1
    while replacement.buffer and event_final_id(replacement.buffer[0]) < expected_id:
        replacement.buffer.popleft()

    if not replacement.buffer:
        return None

    event = replacement.buffer[0]
    if event_first_id(event) <= expected_id <= event_final_id(event):
        return True
    if event_first_id(event) > expected_id:
        return False
    return None


async def process_available_events(session: DepthStreamSession, book: dict, state: dict, max_events=None):
    processed = 0
    while session.buffer and (max_events is None or processed < max_events):
        event = session.buffer.popleft()
        apply_and_record_event(book, event, state, session.name)
        processed += 1
    return processed


async def prepare_replacement_session(
    symbol: str,
    snapshot_limit: int,
    state: dict,
    handoff_index: int,
    shutdown_event=None,
):
    replacement = DepthStreamSession(f"handoff-{handoff_index}", symbol, state)
    await replacement.start()
    try:
        snapshot = await wait_for_syncable_snapshot(
            replacement,
            symbol,
            snapshot_limit,
            state,
            shutdown_event,
        )
        return replacement, int(snapshot["lastUpdateId"])
    except Exception:
        await replacement.close()
        raise


async def perform_handoff(
    active: DepthStreamSession,
    book: dict,
    symbol: str,
    snapshot_limit: int,
    state: dict,
    shutdown_event=None,
):
    handoff_index = state["handoff_count"] + 1
    retry_count = 0

    while True:
        if shutdown_event is not None and shutdown_event.is_set():
            return active

        replacement = None
        try:
            replacement, snapshot_id = await prepare_replacement_session(
                symbol,
                snapshot_limit,
                state,
                handoff_index,
                shutdown_event,
            )
            retry_count = 0
            append_log_line(
                state,
                f"handoff_prepare_ok old={active.name}, new={replacement.name}, snapshot_id={snapshot_id}",
            )

            while True:
                active.ensure_running()
                replacement.ensure_running()

                await process_available_events(active, book, state, max_events=200)

                if int(book["lastUpdateId"]) >= snapshot_id:
                    bridge = replacement_bridge_status(replacement, book)
                    if bridge is True:
                        old_name = active.name
                        await active.close()
                        state["handoff_count"] += 1
                        append_log_line(
                            state,
                            f"handoff_complete old={old_name}, new={replacement.name}, "
                            f"last_update_id={book['lastUpdateId']}",
                        )
                        return replacement
                    if bridge is False:
                        raise SyncGapError(
                            f"{replacement.name}: replacement cannot bridge from "
                            f"{book['lastUpdateId'] + 1}; first buffered U={event_first_id(replacement.buffer[0])}"
                        )

                if shutdown_event is not None and shutdown_event.is_set():
                    await replacement.close()
                    return active

                await asyncio.sleep(0.01)

        except Exception as exc:
            retry_count += 1
            delay = min(RECONNECT_BASE_DELAY_SEC * (2 ** (retry_count - 1)), RECONNECT_MAX_DELAY_SEC)
            state["last_error"] = str(exc)
            append_log_line(
                state,
                f"handoff_retry old={active.name}, error={exc}, retry_count={retry_count}, delay={delay}",
            )
            if replacement is not None:
                await replacement.close()

            wait_until = time.monotonic() + delay
            while time.monotonic() < wait_until:
                active.ensure_running()
                await process_available_events(active, book, state, max_events=200)
                if shutdown_event is not None and shutdown_event.is_set():
                    return active
                await asyncio.sleep(0.1)


async def maintain_local_order_book(
    symbol="BTCUSDT",
    duration_sec=None,
    snapshot_limit=5000,
    max_session_sec=WS_MAX_SESSION_SEC,
    handoff_lead_sec=WS_HANDOFF_LEAD_SEC,
    shutdown_event=None,
):
    book = None
    active = None
    run_deadline = None
    reconnect_count = 0
    state = {
        "start_time": current_unix_time(),
        "last_fetch_time": None,
        "ws_message_count": 0,
        "applied_event_count": 0,
        "skipped_event_count": 0,
        "ignored_message_count": 0,
        "server_shutdown_count": 0,
        "handoff_count": 0,
        "error_count": 0,
        "last_error": None,
        "last_update_id": None,
        "last_update_time": None,
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
                if active is None:
                    active, book = await open_initial_session(symbol, snapshot_limit, state, shutdown_event)
                    reconnect_count = 0
                    state["last_update_id"] = int(book["lastUpdateId"])
                    state["last_update_time"] = float(book["lastUpdateTime"])
                    append_log_line(
                        state,
                        f"collector_synced active={active.name}, last_update_id={book['lastUpdateId']}",
                    )

                processed = await process_available_events(active, book, state, max_events=500)

                if should_start_handoff(active, max_session_sec, handoff_lead_sec):
                    append_log_line(
                        state,
                        f"handoff_start active={active.name}, age={active.age_sec():.3f}, "
                        f"last_update_id={book['lastUpdateId']}",
                    )
                    active = await perform_handoff(
                        active,
                        book,
                        symbol,
                        snapshot_limit,
                        state,
                        shutdown_event,
                    )
                    continue

                active.ensure_running()
                if processed == 0:
                    await asyncio.sleep(0.01)

            except (
                ConnectionError,
                OSError,
                requests.RequestException,
                RuntimeError,
                json.JSONDecodeError,
            ) as exc:
                reconnect_count += 1
                delay = min(RECONNECT_BASE_DELAY_SEC * (2 ** (reconnect_count - 1)), RECONNECT_MAX_DELAY_SEC)
                state["error_count"] += 1
                state["last_error"] = str(exc)
                append_log_line(
                    state,
                    f"collector_error error={exc}, reconnect_count={reconnect_count}, delay={delay}",
                )
                print(f"connection error: {exc}")

                if active is not None:
                    await active.close()
                    active = None

                if run_deadline is not None and time.monotonic() >= run_deadline:
                    break
                if shutdown_event is not None and shutdown_event.is_set():
                    break

                print(f"reconnecting after {delay} sec")
                await asyncio.sleep(delay)

        if book is None:
            raise RuntimeError("collector stopped before building a valid order book")

        return book
    finally:
        if active is not None:
            await active.close()

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
                f"applied_event_count={state['applied_event_count']}, "
                f"skipped_event_count={state['skipped_event_count']}, "
                f"handoff_count={state['handoff_count']}, "
                f"error_count={state['error_count']}, "
                f"last_error={state['last_error']}, "
                f"last_update_id={state['last_update_id']}"
            ),
        )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default=SYMBOL)
    parser.add_argument("--snapshot-limit", type=int, default=SNAPSHOT_LIMIT)
    parser.add_argument("--output-dir", default=OUTPUT_DIR)
    parser.add_argument("--max-session-sec", type=float, default=WS_MAX_SESSION_SEC)
    parser.add_argument("--handoff-lead-sec", type=float, default=WS_HANDOFF_LEAD_SEC)

    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--stdin-stop", action="store_true")
    mode_group.add_argument("--duration-sec", type=int)

    return parser.parse_args()


async def main():
    global OUTPUT_DIR

    args = parse_args()
    OUTPUT_DIR = os.path.abspath(args.output_dir)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    shutdown_event = asyncio.Event()
    stdin_task = None

    if args.duration_sec is not None and args.duration_sec <= 0:
        raise ValueError("duration-sec must be >= 1")
    if args.max_session_sec <= 0:
        raise ValueError("max-session-sec must be > 0")
    if args.handoff_lead_sec < 0:
        raise ValueError("handoff-lead-sec must be >= 0")

    if args.stdin_stop:
        print("type 'stop' on stdin to stop")
        stdin_task = asyncio.create_task(stdin_stop_watcher(shutdown_event))

    try:
        return await maintain_local_order_book(
            symbol=args.symbol,
            duration_sec=args.duration_sec,
            snapshot_limit=args.snapshot_limit,
            max_session_sec=args.max_session_sec,
            handoff_lead_sec=args.handoff_lead_sec,
            shutdown_event=shutdown_event,
        )
    finally:
        shutdown_event.set()
        if stdin_task is not None:
            stdin_task.cancel()
            try:
                await stdin_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    final_book = asyncio.run(main())
    append_final_orderbook(final_book)

    print("saved:")
    print(" - initial_orderbook_YYYY-MM-DD.jsonl")
    print(" - depth_diff_YYYY-MM-DD.jsonl")
    print(" - final_orderbook_YYYY-MM-DD.jsonl")
    print(" - main3_log_YYYY-MM-DD.txt")
