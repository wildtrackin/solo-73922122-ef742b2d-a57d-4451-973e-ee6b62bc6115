"""深度行情采集服务（FastAPI 单进程）。

对齐 Binance Spot 本地盘口维护规范：
  1. 先连 WebSocket 缓存增量 depthUpdate，再取 HTTP 深度快照；
  2. 快照 lastUpdateId 记为 L，增量含起止编号 U/u；
  3. 丢弃 u <= L 的增量；仅当 U <= L+1 <= u 时应用，并令 L = u；
  4. 增量中的数量是该价位的最新总量，0 表示删除该价位；每侧最多保留 100 档；
  5. 缺号（U > L+1）或断线：立即停止发布有效报价并自动重新同步；
  6. 快照失败绝不上报就绪；重连前的在途快照响应不得覆盖新盘口（代际号防护）。

行情停更（连接正常但不再产生推进增量）保护：
  * 每条增量在 WebSocket 实际收到时记录单调时钟（time.monotonic）时间；
  * 只有真正推进 lastUpdateId 的增量才刷新新鲜度；重复消息（u <= L）与
    WebSocket 心跳不会续期；
  * 距最后一次有效推进的“接收时刻”满 STALE_AFTER_SECONDS（3 秒），立即撤下
    报价（synced=false、不返回盘口）并自动重新同步；
  * 缓存增量稍后应用时使用其接收时刻，而不是应用时刻，不能借应用动作重新计时；
  * 首次就绪与重同步恢复都必须满足：快照成功后至少应用了一条“接收至今不足
    3 秒”的推进增量，不能仅凭快照成功就发布报价。
  * 所有状态写入都带代际号检查；停采或旧周期的迟到计时任务不得改动新周期。
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

import httpx
import websockets
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from websockets.exceptions import WebSocketException

SYMBOL = "BTCUSDT"
MAX_LEVELS = 100
MOCK_HTTP = os.getenv("MOCK_HTTP", "http://localhost:8001")
MOCK_WS = os.getenv("MOCK_WS", "ws://localhost:8001/ws/btcusdt@depth")
SNAPSHOT_TIMEOUT = 30.0
RESYNC_BACKOFF = 1.0
# 行情停更阈值：距最后一次“有效推进”的接收时刻达到该值即撤价并重同步。
STALE_AFTER_SECONDS = float(os.getenv("STALE_AFTER_SECONDS", "3.0"))

STATIC_INDEX = Path(__file__).parent / "static" / "index.html"


class GapDetected(Exception):
    """增量编号与本地 L 不连续（U > L+1）。"""


class ResyncRequired(Exception):
    """连接中断等需要重新同步的情况。"""


class StaleData(ResyncRequired):
    """连接仍正常，但长时间没有推进盘口编号的增量（行情停更）。"""


class OrderBook:
    """本地盘口：数量为价位最新总量，0 删除；每侧最多 MAX_LEVELS 档。

    ``last_advance_recv_mono`` 记录最后一条“真正推进 L”的增量在 WebSocket
    被实际接收时的单调时钟时间。它只由 apply(..., recv_mono=...) 且确实推进
    编号时更新，因此重复消息/心跳/缓存旧消息都不能让数据重新变新鲜。
    """

    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_update_id: int | None = None  # L
        self.last_advance_recv_mono: float | None = None
        self.last_advance_update_id: int | None = None

    def load_snapshot(self, snapshot: dict) -> None:
        self.bids = {float(p): float(q) for p, q in snapshot["bids"]}
        self.asks = {float(p): float(q) for p, q in snapshot["asks"]}
        self.last_update_id = int(snapshot["lastUpdateId"])
        # 快照本身不算一次“推进增量”，就绪必须等待后续增量。
        self.last_advance_recv_mono = None
        self.last_advance_update_id = None
        self._trim()

    def apply(self, event: dict, stats: dict, recv_mono: float | None = None) -> bool:
        """应用一条增量；返回是否真正推进了 lastUpdateId。"""
        U, u = int(event["U"]), int(event["u"])
        L = self.last_update_id
        assert L is not None, "快照未加载"
        if u <= L:
            stats["dropped"] += 1  # 重复/过期消息，直接丢弃，不能刷新新鲜度
            return False
        if U > L + 1:
            raise GapDetected(f"缺号: U={U} > L+1={L + 1}")
        # 满足 U <= L+1 <= u，应用并推进 L
        for p, q in event.get("b", []):
            self._set(self.bids, float(p), float(q))
        for p, q in event.get("a", []):
            self._set(self.asks, float(p), float(q))
        self.last_update_id = u
        stats["applied"] += 1
        if recv_mono is not None:
            # 注意：用接收时刻，不用应用时刻。缓存增量稍后应用也不会重新计时。
            self.last_advance_recv_mono = recv_mono
            self.last_advance_update_id = u
        self._trim()
        return True

    def has_fresh_advance(self, now_mono: float, max_age: float = STALE_AFTER_SECONDS) -> bool:
        """快照后是否已应用至少一条接收至今不足 max_age 的推进增量。"""
        return (
            self.last_advance_recv_mono is not None
            and (now_mono - self.last_advance_recv_mono) < max_age
        )

    def data_age(self, now_mono: float) -> float | None:
        if self.last_advance_recv_mono is None:
            return None
        return max(0.0, now_mono - self.last_advance_recv_mono)

    @staticmethod
    def _set(side: dict[float, float], price: float, qty: float) -> None:
        if qty == 0.0:
            side.pop(price, None)
        else:
            side[price] = qty

    def _trim(self) -> None:
        if len(self.bids) > MAX_LEVELS:
            self.bids = dict(sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:MAX_LEVELS])
        if len(self.asks) > MAX_LEVELS:
            self.asks = dict(sorted(self.asks.items(), key=lambda kv: kv[0])[:MAX_LEVELS])

    def top(self, n: int) -> tuple[list[list[str]], list[list[str]]]:
        bids = [[f"{p:.1f}", f"{q:.4f}"]
                for p, q in sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:n]]
        asks = [[f"{p:.1f}", f"{q:.4f}"]
                for p, q in sorted(self.asks.items(), key=lambda kv: kv[0])[:n]]
        return bids, asks


class CollectorState:
    def __init__(self) -> None:
        self.running = False
        self.status = "stopped"               # stopped / syncing / synced / resyncing
        self.book: OrderBook | None = None    # 仅在 synced 时对外发布，其余时间为 None
        self.generation = 0                   # 代际号：防止在途旧响应/旧计时任务覆盖新盘口
        self.task: asyncio.Task | None = None
        self.last_error: str | None = None
        # 最后一条有效推进增量的实际接收时刻（单调时钟），用于页面展示数据年龄。
        self.last_advance_mono: float | None = None
        self.last_advance_update_id: int | None = None
        # 行情停更/连接中断/缺号等原因，供页面显示。
        self.pause_reason: str | None = None
        self.stats = {
            "applied": 0,            # 已应用增量
            "dropped": 0,            # 丢弃的重复/过期消息
            "ignored": 0,            # 忽略的非 depthUpdate 消息（如心跳）
            "gaps": 0,               # 缺号次数
            "resyncs": 0,            # 重同步次数
            "stale": 0,              # 因行情停更而撤价次数
            "snapshotAttempts": 0,   # 快照尝试次数
            "snapshotOk": 0,         # 快照成功次数
        }


state = CollectorState()


async def fetch_snapshot() -> dict:
    state.stats["snapshotAttempts"] += 1
    async with httpx.AsyncClient(timeout=SNAPSHOT_TIMEOUT) as client:
        resp = await client.get(
            f"{MOCK_HTTP}/api/v3/depth",
            params={"symbol": SYMBOL, "limit": MAX_LEVELS},
        )
        resp.raise_for_status()  # 快照失败：抛错，外层重试，期间不就绪
        data = resp.json()
    state.stats["snapshotOk"] += 1
    return data


def _withdraw(reason: str) -> None:
    """立即撤下对外报价，并记录停更原因。"""
    state.book = None
    state.status = "resyncing"
    state.pause_reason = reason


async def sync_cycle() -> None:
    """一轮「连接 → 缓存增量 → 取快照 → 对齐 → 直播」周期；异常交由外层重同步。"""
    state.generation += 1
    gen = state.generation
    state.status = "syncing"
    # 注意：不清空 pause_reason/last_error，让运维在重试期间仍能看到上次停更原因；
    # 成功发布（synced）时才清空。

    # 每条记录 = (增量消息, WebSocket 实际接收时的单调时钟时间)
    buffer: list[tuple[dict, float]] = []
    closed = asyncio.Event()
    item_ready = asyncio.Event()

    async def pump(ws) -> None:
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except (ValueError, TypeError):
                    continue
                # WebSocket 心跳/非 depthUpdate 消息不能进入盘口，也不能续期。
                if not isinstance(msg, dict) or msg.get("e") != "depthUpdate":
                    state.stats["ignored"] += 1
                    continue
                if "U" not in msg or "u" not in msg:
                    state.stats["ignored"] += 1
                    continue
                recv_mono = time.monotonic()
                buffer.append((msg, recv_mono))
                item_ready.set()
        finally:
            closed.set()
            item_ready.set()

    async with websockets.connect(MOCK_WS, ping_interval=20, ping_timeout=20) as ws:
        pump_task = asyncio.create_task(pump(ws))
        fetch_task: asyncio.Task | None = None
        try:
            # 步骤一：pump 已在缓存 WebSocket 增量；步骤二：并发取 HTTP 快照
            fetch_task = asyncio.create_task(fetch_snapshot())
            closed_task = asyncio.create_task(closed.wait())
            await asyncio.wait({fetch_task, closed_task}, return_when=asyncio.FIRST_COMPLETED)
            closed_task.cancel()
            if closed.is_set():
                # 快照在途期间断线：作废本次在途响应，直接重同步
                fetch_task.cancel()
                raise ResyncRequired("快照在途期间连接断开")
            snapshot = fetch_task.result()  # 快照失败在此抛错，绝不将就绪置真
            if gen != state.generation:
                return  # 在途旧响应不得覆盖新盘口
            snapshot_mono = time.monotonic()

            book = OrderBook()
            book.load_snapshot(snapshot)

            # 步骤三：对齐缓存增量，随后进入直播
            i = 0
            published = False
            while True:
                # 应用所有已缓存增量；每次都把“接收时刻”交给盘口。
                while i < len(buffer):
                    event, recv_mono = buffer[i]
                    advanced = book.apply(event, state.stats, recv_mono=recv_mono)
                    i += 1
                    if advanced and gen == state.generation:
                        state.last_advance_mono = recv_mono
                        state.last_advance_update_id = book.last_update_id

                if closed.is_set():
                    raise ResyncRequired("连接断开")

                now = time.monotonic()
                if not published and book.has_fresh_advance(now):
                    # 首次就绪/重同步恢复：快照之后必须已有接收不足 3 秒的推进增量。
                    if gen != state.generation:
                        return
                    state.book = book
                    state.status = "synced"
                    state.last_error = None
                    state.pause_reason = None
                    published = True

                # 计算本周期下一次必须检查的时刻：
                #   已发布 → 最后有效推进的接收时刻 + 3 秒；
                #   未发布但已有（已过期的）推进增量 → 同样是它 + 3 秒（立即重同步）；
                #   未发布且尚无任何推进增量 → 快照完成后 3 秒仍无新鲜增量则重同步。
                if book.last_advance_recv_mono is not None:
                    deadline = book.last_advance_recv_mono + STALE_AFTER_SECONDS
                else:
                    deadline = snapshot_mono + STALE_AFTER_SECONDS
                remaining = deadline - now
                if remaining <= 0:
                    age = now - (
                        book.last_advance_recv_mono
                        if book.last_advance_recv_mono is not None
                        else snapshot_mono
                    )
                    if published:
                        reason = (
                            f"行情停更：距最后有效推进（u={book.last_advance_update_id}）"
                            f"已 {age:.1f}s（阈值 {STALE_AFTER_SECONDS:.0f}s），已撤下报价"
                        )
                    else:
                        reason = (
                            f"行情停更：快照后 {age:.1f}s 未收到接收至今不足 "
                            f"{STALE_AFTER_SECONDS:.0f}s 的推进增量，不就绪"
                        )
                    if gen != state.generation:
                        return  # 旧周期迟到的计时结果不得改动新周期
                    _withdraw(reason)
                    state.stats["stale"] += 1
                    raise StaleData(reason)

                item_ready.clear()
                if i < len(buffer) or closed.is_set():
                    continue
                try:
                    # 精确等到下一条增量或停更阈值；超时后回到循环顶部复查。
                    await asyncio.wait_for(item_ready.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass
        finally:
            pump_task.cancel()
            if fetch_task is not None and not fetch_task.done():
                fetch_task.cancel()  # 停止/重同步时取消在途快照请求


async def collector_loop() -> None:
    while state.running:
        try:
            await sync_cycle()
        except asyncio.CancelledError:
            raise
        except GapDetected as exc:
            state.stats["gaps"] += 1
            state.last_error = str(exc)
            state.pause_reason = f"缺号，已停止发布并重新同步：{exc}"
        except StaleData as exc:
            state.last_error = str(exc)
            # pause_reason 已由 _withdraw 设置
        except (ResyncRequired, WebSocketException, OSError) as exc:
            state.last_error = f"连接问题: {exc}"
            state.pause_reason = f"连接中断，已停止发布并重新同步：{exc}"
        except httpx.HTTPError as exc:
            state.last_error = f"快照失败: {exc}"
            state.pause_reason = f"快照失败，未就绪并重试：{exc}"
        except Exception as exc:  # noqa: BLE001
            state.last_error = f"未预期错误: {exc!r}"
            state.pause_reason = f"未预期错误，已停止发布并重新同步：{exc!r}"
        # 缺号/断线/停更/快照失败：立即停止发布有效报价
        state.book = None
        if state.running:
            state.status = "resyncing"
            state.stats["resyncs"] += 1
            await asyncio.sleep(RESYNC_BACKOFF)
    state.status = "stopped"
    state.book = None


async def start_collection() -> None:
    if state.running:
        return
    state.running = True
    state.last_error = None
    state.pause_reason = None
    state.last_advance_mono = None
    state.last_advance_update_id = None
    state.task = asyncio.create_task(collector_loop())


async def stop_collection() -> None:
    # 先推进代际号：任何旧周期残留的计时/响应都会被判为非当前周期。
    state.generation += 1
    state.running = False
    if state.task is not None:
        state.task.cancel()
        try:
            await state.task
        except asyncio.CancelledError:
            pass
        state.task = None
    state.status = "stopped"
    state.book = None
    state.last_advance_mono = None
    state.last_advance_update_id = None
    state.pause_reason = None


# ------------------------------- HTTP API -------------------------------

app = FastAPI(title="Depth Collector")


class ControlBody(BaseModel):
    action: str  # start / stop


class ScenarioBody(BaseModel):
    snapshot_delay_ms: int = 0
    snapshot_fail: bool = False
    duplicates: bool = False
    gap: bool = False
    disconnect: bool = False
    silent: bool = False          # 保持连接但静默（只发心跳，不推增量）
    repeat_last: bool = False     # 反复重发末条增量（编号不推进）


@app.get("/")
async def index():
    return FileResponse(STATIC_INDEX)


@app.post("/api/control")
async def api_control(body: ControlBody):
    if body.action == "start":
        await start_collection()
    elif body.action == "stop":
        await stop_collection()
    else:
        return JSONResponse(status_code=400, content={"error": "action 须为 start/stop"})
    return {"ok": True, "running": state.running}


@app.get("/api/state")
async def api_state():
    synced = state.status == "synced" and state.book is not None
    now = time.monotonic()
    age = None if state.last_advance_mono is None else max(0.0, now - state.last_advance_mono)
    if synced:
        bids, asks = state.book.top(5)
        last_update_id = state.book.last_update_id
        levels = {"bids": len(state.book.bids), "asks": len(state.book.asks)}
    else:
        # 未同步：不发布任何报价，下游不得使用
        bids, asks, last_update_id, levels = [], [], None, {"bids": 0, "asks": 0}
    return {
        "running": state.running,
        "status": state.status,
        "synced": synced,
        "symbol": SYMBOL,
        "lastUpdateId": last_update_id,
        "lastAdvanceUpdateId": state.last_advance_update_id,
        "bids": bids,
        "asks": asks,
        "levels": levels,
        # 数据年龄 = 距最后一条“有效推进”增量被实际接收的秒数（单调时钟）。
        "dataAgeSeconds": None if age is None else round(age, 3),
        "dataAgeMs": None if age is None else int(age * 1000),
        "staleAfterSeconds": STALE_AFTER_SECONDS,
        # 停更/断线/缺号/快照失败原因，页面直接展示。
        "pauseReason": state.pause_reason,
        "stats": state.stats,
        "lastError": state.last_error,
        "source": {"http": MOCK_HTTP, "ws": MOCK_WS},
    }


@app.get("/api/scenario")
async def api_get_scenario():
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{MOCK_HTTP}/scenario")
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPError as exc:
        return JSONResponse(status_code=502, content={"error": f"行情源不可达: {exc}"})


@app.post("/api/scenario")
async def api_set_scenario(body: ScenarioBody):
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(f"{MOCK_HTTP}/scenario", json=body.model_dump())
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPError as exc:
        return JSONResponse(status_code=502, content={"error": f"行情源不可达: {exc}"})
