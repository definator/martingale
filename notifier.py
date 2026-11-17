"""
notifier.py – Telegram notifications via python-telegram-bot (async).

Key change: all send() calls go through an internal asyncio.Queue.
A background worker drains the queue independently so that Telegram
latency never blocks the critical trading path.
"""

import asyncio
from loguru import logger

try:
    from telegram import Bot
    from telegram.error import TelegramError
    TELEGRAM_OK = True
except ImportError:
    TELEGRAM_OK = False

# Maximum queued notifications before old ones are dropped (prevents unbounded growth)
_QUEUE_MAX = 200


class Notifier:
    def __init__(self, enabled: bool, bot_token: str, chat_id: str):
        self.enabled = enabled and TELEGRAM_OK
        self.chat_id = chat_id
        self._bot: "Bot | None" = None
        if self.enabled:
            self._bot = Bot(token=bot_token)

        # Internal async queue – trading code enqueues, worker sends
        self._queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=_QUEUE_MAX)
        self._worker_task: asyncio.Task | None = None

    # ── Lifecycle ─────────────────────────────────────────────

    def start_worker(self):
        """Start the background sender task. Call once from the asyncio event loop."""
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._worker(), name="notifier_worker")

    async def stop_worker(self):
        """Drain queue and stop the background sender."""
        if self._worker_task and not self._worker_task.done():
            await self._queue.put(None)   # sentinel
            try:
                await asyncio.wait_for(self._worker_task, timeout=10.0)
            except asyncio.TimeoutError:
                self._worker_task.cancel()

    async def _worker(self):
        """Background coroutine: dequeue and send Telegram messages one at a time."""
        while True:
            try:
                text = await self._queue.get()
                if text is None:          # sentinel → stop
                    self._queue.task_done()
                    return
                await self._send_direct(text)
                self._queue.task_done()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("Notifier worker error: {}", e)

    # ── Public API ────────────────────────────────────────────

    def enqueue(self, text: str):
        """
        Non-blocking enqueue. Safe to call from any coroutine on the critical path.
        If the queue is full, the message is dropped (not traded, not the end of the world).
        """
        if not self.enabled and self._bot is None:
            logger.info("[TG] {}", text[:200])
            return
        try:
            self._queue.put_nowait(text)
        except asyncio.QueueFull:
            logger.warning("Notifier queue full – dropped message: {}…", text[:80])

    async def send(self, text: str):
        """
        Async send – enqueues the message (returns immediately).
        Kept for backward compatibility; does NOT await actual delivery.
        """
        self.enqueue(text)

    async def _send_direct(self, text: str):
        """Actually transmit to Telegram (called only by the worker)."""
        if not self.enabled or self._bot is None:
            logger.info("[TG] {}", text[:200])
            return
        try:
            await self._bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode="HTML",
            )
        except Exception as e:
            logger.warning("Telegram send failed: {}", e)

    # ── Convenience wrappers ──────────────────────────────────

    async def trade_opened(self, symbol: str, direction: str, entry_price: float,
                            deposit: float, tp_price: float, try_num: int, max_tries: int,
                            expected_profit: float):
        direction_emoji = "🟢 LONG" if direction.lower() == "long" else "🔴 SHORT"
        await self.send(
            f"<b>🚀 Сделка открыта</b>\n"
            f"Монета: <code>{symbol}</code>  {direction_emoji}\n"
            f"Попытка: {try_num}/{max_tries}\n"
            f"Депозит: <code>{deposit:.2f} USDT</code>\n"
            f"Цена входа: <code>{entry_price:.6g}</code>\n"
            f"Тейк-профит: <code>{tp_price:.6g}</code>\n"
            f"Ожидаемая прибыль при TP: <code>{expected_profit:.2f} USDT</code>"
        )

    async def limit_order_filled(self, symbol: str, order_index: int, orders_qty: int,
                                  fill_price: float, avg_entry: float, tp_price: float,
                                  tp_pct: float, total_deposit: float, expected_profit: float):
        await self.send(
            f"<b>📌 Исполнен ордер {order_index}/{orders_qty}</b>\n"
            f"Монета: <code>{symbol}</code>\n"
            f"Цена ордера: <code>{fill_price:.6g}</code>\n"
            f"Средняя точка входа: <code>{avg_entry:.6g}</code>\n"
            f"Тейк-профит: <code>{tp_price:.6g}</code>\n"
            f"Разворот до тейка: <code>{tp_pct:.2f}%</code>\n"
            f"Суммарный депозит: <code>{total_deposit:.2f} USDT</code>\n"
            f"Ожидаемая прибыль при TP: <code>{expected_profit:.2f} USDT</code>"
        )

    async def take_profit_hit(self, symbol: str, pnl: float, balance: float,
                               try_num: int, max_tries: int):
        text = (
            f"<b>✅ Сделка закрыта (Тейк-профит исполнен)</b>\n"
            f"Монета: <code>{symbol}</code>\n"
            f"Попытка: {try_num}/{max_tries}\n"
            f"Прибыль: <code>{pnl:+.4f} USDT</code>\n"
            f"Всего на балансе: <code>{balance:.4f} USDT</code>"
        )
        # Send immediately and concurrently (bypass queue to ensure no delay, without blocking)
        asyncio.create_task(self._send_direct(text))

    async def error(self, symbol: str, msg: str):
        await self.send(
            f"<b>⚠️ Ошибка</b>  <code>{symbol}</code>\n{msg}"
        )

    async def info(self, msg: str):
        await self.send(msg)
