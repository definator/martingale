"""
notifier.py – Telegram notifications via python-telegram-bot (async).
"""

import asyncio
from loguru import logger

try:
    from telegram import Bot
    from telegram.error import TelegramError
    TELEGRAM_OK = True
except ImportError:
    TELEGRAM_OK = False


class Notifier:
    def __init__(self, enabled: bool, bot_token: str, chat_id: str):
        self.enabled = enabled and TELEGRAM_OK
        self.chat_id = chat_id
        self._bot: "Bot | None" = None
        if self.enabled:
            self._bot = Bot(token=bot_token)

    async def send(self, text: str):
        if not self.enabled or self._bot is None:
            logger.info("[TG] {}", text)
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
        await self.send(
            f"<b>✅ Сделка закрыта (Тейк-профит исполнен)</b>\n"
            f"Монета: <code>{symbol}</code>\n"
            f"Попытка: {try_num}/{max_tries}\n"
            f"Прибыль: <code>{pnl:+.4f} USDT</code>\n"
            f"Всего на балансе: <code>{balance:.4f} USDT</code>"
        )

    async def error(self, symbol: str, msg: str):
        await self.send(
            f"<b>⚠️ Ошибка</b>  <code>{symbol}</code>\n{msg}"
        )

    async def info(self, msg: str):
        await self.send(msg)

