"""
symbol_trader.py – Stateful trader for a single symbol.

Manages the full lifecycle:
  open_position → place_grid → wait (WS events + REST polling) → tp_hit → cleanup → repeat
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from binance import AsyncClient, BinanceSocketManager
from loguru import logger

import db
import exchange as ex
import strategy as strat
from notifier import Notifier

if TYPE_CHECKING:
    pass


class SymbolTrader:
    """
    Manages one symbol through up to `max_tries` cycles.
    Each cycle = open → grid → TP → close.
    """

    def __init__(
        self,
        client: AsyncClient,
        bsm: BinanceSocketManager,
        notifier: Notifier,
        symbol_cfg: dict,
    ):
        self.client   = client
        self.bsm      = bsm
        self.notifier = notifier
        self.cfg      = symbol_cfg

        # shorthand config fields
        self.symbol               = symbol_cfg["symbol"]
        self.start_deposit        = float(symbol_cfg["start_deposit"])
        self.take_profit          = float(symbol_cfg["take_profit"])
        self.direction            = symbol_cfg["direction"].lower()
        self.distance_pct         = float(symbol_cfg["distance_pct"])
        self.multiplier           = float(symbol_cfg["multiplier"])
        self.orders_qty           = int(symbol_cfg["orders_qty"])
        self.max_tries            = int(symbol_cfg["max_tries"])
        self.trailing_take_profit = bool(symbol_cfg.get("trailing_take_profit", False))

        # exchange meta (populated during startup)
        self.leverage:         int   = 1
        self.tick_size:        float = 0.01
        self.step_size:        float = 0.001
        self.price_precision:  int   = 2
        self.qty_precision:    int   = 3
        self.min_qty:          float = 0.001
        self.min_notional:     float = 5.0

        # runtime state (reset each cycle)
        self.trade_id:      int | None = None
        self.try_number:    int        = 0
        self.initial_entry: float      = 0.0

        # filled orders: list of (entry_price, qty, order_index)
        self.filled_orders: list[tuple[float, float, int]] = []

        # avg entry and TP
        self.avg_entry:   float = 0.0
        self.tp_price:    float = 0.0
        self.tp_order_id: str | None = None

        # grid prices & sizes (set once per cycle)
        self.grid_prices:      list[float] = []
        self.grid_sizes_usdt:  list[float] = []

        # total qty accumulated from all filled limit orders (for TP sizing)
        self.total_qty: float = 0.0

        # WS order update event queue & flags
        self._order_event_queue: asyncio.Queue = asyncio.Queue()
        self._stop = False
        self._tp_hit = False

    # ─────────────────────────────────────────────────────────
    # STARTUP – load exchange metadata
    # ─────────────────────────────────────────────────────────

    async def initialize(self):
        """Load exchange filters and set leverage + margin mode."""
        sym_info = await ex.get_symbol_info(self.client, self.symbol)
        filters  = ex.extract_filters(sym_info)

        self.tick_size       = filters["tick_size"] or 0.0001
        self.step_size       = filters["step_size"] or 0.001
        self.price_precision = filters["price_precision"]
        self.qty_precision   = filters["qty_precision"]
        self.min_qty         = filters["min_qty"] or 0.001
        self.min_notional    = filters["min_notional"] or 5.0

        # Set Cross margin
        try:
            await ex.set_cross_margin(self.client, self.symbol)
            logger.info("{}: margin mode set to CROSS", self.symbol)
        except Exception as e:
            logger.warning("{}: set cross margin: {}", self.symbol, e)

        # Max leverage
        self.leverage = await ex.get_max_leverage(self.client, self.symbol)
        await ex.set_leverage(self.client, self.symbol, self.leverage)
        logger.info("{}: leverage set to {}x", self.symbol, self.leverage)

    # ─────────────────────────────────────────────────────────
    # RECOVERY – check for dangling state on startup
    # ─────────────────────────────────────────────────────────

    async def recover(self) -> bool:
        """
        Check DB for an open trade on this symbol.
        Returns True if a trade was found and recovery attempted.
        """
        open_trades = await db.get_open_trades(self.symbol)
        if not open_trades:
            return False

        trade = open_trades[0]  # assume single open trade per symbol
        self.trade_id   = trade["id"]
        self.try_number = trade["try_number"]
        logger.info("{}: found open trade id={} try={}", self.symbol, self.trade_id, self.try_number)

        # Check if position still exists on exchange
        pos = await ex.get_futures_position(self.client, self.symbol)
        if pos is None:
            # Position closed while bot was offline
            logger.warning("{}: position gone, cleaning up orphan orders…", self.symbol)
            await ex.cancel_all_open_orders(self.client, self.symbol)
            await db.close_trade(self.trade_id, realized_pnl=0.0)
            await self.notifier.info(
                f"⚠️ <code>{self.symbol}</code> — обнаружена закрытая позиция при перезапуске.\n"
                f"Осиротевшие ордера отменены."
            )
            self.trade_id = None
            return True

        # Position exists – restore state from DB
        orders = await db.get_orders_for_trade(self.trade_id)
        self.filled_orders = []
        for o in orders:
            if o["status"] == "FILLED" and o["order_type"] in ("MARKET", "LIMIT"):
                self.filled_orders.append((o["price"] or 0.0, o["qty"] or 0.0, o["order_index"]))

        if self.filled_orders:
            self.filled_orders.sort(key=lambda x: x[2])
            self.initial_entry = self.filled_orders[0][0]
            self.avg_entry = strat.compute_average_entry(
                [(p, q) for p, q, _ in self.filled_orders]
            )
            self.total_qty = strat.total_filled_qty(
                [(p, q) for p, q, _ in self.filled_orders]
            )
            if self.trailing_take_profit:
                self.tp_price = strat.compute_tp_price(
                    self.avg_entry, self.take_profit, self.direction
                )
            else:
                self.tp_price = strat.compute_tp_price(
                    self.initial_entry, self.take_profit, self.direction
                )

        logger.info("{}: recovered trade – initial={:.6g} avg_entry={:.6g} tp={:.6g} trailing={} qty={}",
                    self.symbol, self.initial_entry, self.avg_entry, self.tp_price,
                    self.trailing_take_profit, self.total_qty)
        await self.notifier.info(
            f"🔄 <code>{self.symbol}</code> — восстановление после перезапуска.\n"
            f"Начальный вход: <code>{self.initial_entry:.6g}</code>, Средняя входа: <code>{self.avg_entry:.6g}</code>, TP: <code>{self.tp_price:.6g}</code>"
        )
        return True

    # ─────────────────────────────────────────────────────────
    # MAIN LOOP
    # ─────────────────────────────────────────────────────────

    async def run(self):
        """Entry point: run up to max_tries trade cycles."""
        already_open = await self.recover()

        if already_open and self.trade_id is not None:
            # Resume waiting for TP on the recovered trade
            await self._wait_for_tp()
            self.try_number += 1

        while self.try_number < self.max_tries and not self._stop:
            self.try_number += 1
            await self._run_cycle()

        logger.info("{}: all {} tries completed.", self.symbol, self.max_tries)
        await self.notifier.info(
            f"🏁 <code>{self.symbol}</code> — все {self.max_tries} попыток завершены."
        )

    async def _run_cycle(self):
        """Execute one full trade cycle."""
        logger.info("{}: starting cycle {}/{}", self.symbol, self.try_number, self.max_tries)

        # Reset per-cycle state
        self._tp_hit       = False
        self.filled_orders = []
        self.total_qty     = 0.0
        self.avg_entry     = 0.0
        self.initial_entry = 0.0
        self.tp_price      = 0.0
        self.tp_order_id   = None
        self.trade_id      = None

        try:
            # 1. Check balance BEFORE any action
            ok, available, required = await self._check_balance()
            if not ok:
                msg = (f"Недостаточно баланса для открытия позиции.\n"
                       f"Нужно: {required:.2f} USDT маржи\n"
                       f"Доступно: {available:.2f} USDT")
                logger.error("{}: {}", self.symbol, msg.replace('\n', ' '))
                await self.notifier.error(self.symbol, msg)
                return

            # 2. Enter market
            await self._open_market_entry()

            # 3. Place grid
            await self._place_grid_orders()

            # 4. Wait for TP (blocking – processes WS events + REST polling)
            await self._wait_for_tp()

        except asyncio.CancelledError:
            logger.info("{}: cycle cancelled.", self.symbol)
            raise
        except Exception as e:
            logger.exception("{}: cycle error: {}", self.symbol, e)
            await self.notifier.error(self.symbol, f"Ошибка цикла: {e}")
        finally:
            # Ensure all remaining limit & algo orders are canceled upon cycle exit
            await ex.cancel_all_open_orders(self.client, self.symbol)

    # ─────────────────────────────────────────────────────────
    # BALANCE CHECK
    # ─────────────────────────────────────────────────────────

    async def _check_balance(self) -> tuple[bool, float, float]:
        """
        Returns (ok, available_balance, required_margin).
        Uses real mark price to compute accurate order sizes.
        """
        mark_price = await ex.get_mark_price(self.client, self.symbol)
        sizes_usdt = strat.compute_order_sizes(
            self.start_deposit, self.multiplier, self.orders_qty
        )
        # Margin required = total notional / leverage
        required = sum(sizes_usdt) / self.leverage
        available = await ex.get_futures_balance(self.client)
        logger.info("{}: balance check — need {:.4f} USDT margin ({}x lev, total notional {:.2f}), have {:.4f} USDT",
                    self.symbol, required, self.leverage, sum(sizes_usdt), available)
        return available >= required, available, required

    # ─────────────────────────────────────────────────────────
    # MARKET ENTRY
    # ─────────────────────────────────────────────────────────

    async def _open_market_entry(self):
        side = "BUY" if self.direction == "long" else "SELL"

        mark_price = await ex.get_mark_price(self.client, self.symbol)
        qty_raw    = self.start_deposit / mark_price
        qty        = ex.round_qty(qty_raw, self.step_size)
        if qty < self.min_qty:
            qty = self.min_qty
        qty = ex.ensure_min_notional(qty, mark_price, self.min_notional, self.step_size)

        # Create DB trade record
        self.trade_id = await db.create_trade(
            self.symbol, self.direction, self.try_number, self.start_deposit
        )

        # Place market order
        order = await ex.place_market_order(
            self.client, self.symbol, side, qty, self.qty_precision
        )
        order_id = str(order["orderId"])

        # Query fill price
        await asyncio.sleep(0.5)
        filled_order = await ex.get_order(self.client, self.symbol, order_id)
        self.avg_entry     = float(filled_order.get("avgPrice") or filled_order.get("price") or mark_price)
        self.initial_entry = self.avg_entry
        filled_qty         = float(filled_order.get("executedQty") or qty)

        # Store
        self.filled_orders.append((self.avg_entry, filled_qty, 1))
        self.total_qty = filled_qty

        await db.create_order_record(
            self.trade_id, order_id, self.symbol, side, "MARKET",
            self.avg_entry, filled_qty, 1
        )
        await db.update_order_status(order_id, "FILLED", datetime.now(timezone.utc).isoformat())
        await db.update_trade(
            self.trade_id,
            entry_price=self.avg_entry,
            entry_qty=filled_qty,
        )

        # Compute TP price and initial expected profit
        self.tp_price = strat.compute_tp_price(self.avg_entry, self.take_profit, self.direction)
        await db.update_trade(self.trade_id, tp_price=self.tp_price)

        expected_profit = strat.expected_profit_usdt(
            [(self.avg_entry, filled_qty)], self.tp_price, self.direction
        )

        logger.info("{}: market entry filled price={:.6g} qty={:.4f} tp={:.6g} trailing={}",
                    self.symbol, self.avg_entry, filled_qty, self.tp_price, self.trailing_take_profit)
        await self.notifier.trade_opened(
            symbol=self.symbol,
            direction=self.direction,
            entry_price=self.avg_entry,
            deposit=self.start_deposit,
            tp_price=self.tp_price,
            try_num=self.try_number,
            max_tries=self.max_tries,
            expected_profit=expected_profit,
        )

    # ─────────────────────────────────────────────────────────
    # GRID PLACEMENT
    # ─────────────────────────────────────────────────────────

    async def _place_grid_orders(self):
        """Compute and place all limit grid orders + the take-profit order."""
        # Grid prices (index 0 = start_price, already filled)
        self.grid_prices = strat.compute_grid_prices(
            self.avg_entry, self.distance_pct, self.orders_qty, self.direction
        )

        # Sizes in USDT notional per grid level
        self.grid_sizes_usdt = strat.compute_order_sizes(
            self.start_deposit, self.multiplier, self.orders_qty
        )

        limit_side = "BUY" if self.direction == "long" else "SELL"

        for i in range(1, self.orders_qty):
            raw_price  = self.grid_prices[i]
            price      = ex.round_price(raw_price, self.tick_size)
            usdt_size  = self.grid_sizes_usdt[i]
            qty_raw    = usdt_size / price
            qty        = ex.round_qty(qty_raw, self.step_size)
            if qty < self.min_qty:
                qty = self.min_qty
            qty = ex.ensure_min_notional(qty, price, self.min_notional, self.step_size)

            try:
                order = await ex.place_limit_order(
                    self.client, self.symbol, limit_side,
                    price, qty, self.price_precision, self.qty_precision
                )
                order_id = str(order["orderId"])
                await db.create_order_record(
                    self.trade_id, order_id, self.symbol, limit_side, "LIMIT",
                    price, qty, i + 1
                )
                logger.debug("{}: limit order {} placed price={:.6g} qty={:.4f}",
                             self.symbol, i + 1, price, qty)
            except Exception as e:
                logger.error("{}: failed to place limit order {}: {}", self.symbol, i + 1, e)

            await asyncio.sleep(0.05)

        # Place initial take-profit
        await self._place_take_profit()

    async def _place_take_profit(self):
        """Place (or replace) the TAKE_PROFIT_MARKET Algo order (closePosition=true)."""
        # Cancel existing TP if any
        if self.tp_order_id:
            await ex.cancel_algo_order(self.client, self.symbol, self.tp_order_id)
            self.tp_order_id = None

        tp_price_rounded = ex.round_price(self.tp_price, self.tick_size)
        tp_side = "SELL" if self.direction == "long" else "BUY"

        try:
            res = await ex.place_take_profit_algo(
                self.client, self.symbol, tp_side,
                tp_price_rounded, self.price_precision
            )
            algo_id = res.get("algoId") or res.get("result", {}).get("algoId")
            if algo_id:
                self.tp_order_id = str(algo_id)
                await db.update_trade(
                    self.trade_id,
                    tp_order_id=self.tp_order_id,
                    tp_price=self.tp_price,
                )
                await db.create_order_record(
                    self.trade_id, self.tp_order_id, self.symbol,
                    tp_side, "TAKE_PROFIT_MARKET",
                    tp_price_rounded, 0, 0
                )
                logger.info("{}: TP Algo order placed stopPrice={:.6g} (algoId={})",
                            self.symbol, tp_price_rounded, self.tp_order_id)
            else:
                logger.error("{}: TP Algo order returned unexpected payload: {}", self.symbol, res)
        except Exception as e:
            logger.error("{}: failed to place TP Algo order: {}", self.symbol, e)

    # ─────────────────────────────────────────────────────────
    # WAIT FOR TP  (WebSocket event loop + REST Polling Backup)
    # ─────────────────────────────────────────────────────────

    async def _wait_for_tp(self):
        """
        Subscribe to user-data stream and process events.
        Also runs periodic REST polling backup every 5s for resilience.
        """
        logger.info("{}: waiting for TP (listening to user-data WS + polling backup)…", self.symbol)

        async with ex.get_user_data_stream(self.bsm) as stream:
            last_poll = time.time()
            while not self._stop and not self._tp_hit:
                try:
                    msg = await asyncio.wait_for(stream.recv(), timeout=2.0)
                    await self._handle_ws_message(msg)
                except asyncio.TimeoutError:
                    pass
                except Exception as e:
                    logger.warning("{}: WS error: {}", self.symbol, e)
                    await asyncio.sleep(1.0)

                if self._tp_hit:
                    break

                # Periodic REST polling fallback
                now = time.time()
                if now - last_poll >= 5.0:
                    last_poll = now
                    await self._poll_check_state()

    async def _poll_check_state(self):
        """Fail-safe check via REST API for missed fills or closed position."""
        if self._stop or self._tp_hit:
            return

        try:
            # 1. Check position state
            pos = await ex.get_futures_position(self.client, self.symbol)
            if self.filled_orders and pos is None:
                logger.info("{}: REST poll detected flat position (TP hit)", self.symbol)
                await self._on_tp_filled(self.tp_price, self.total_qty)
                return

            # 2. Check open limit orders in DB vs exchange
            if self.trade_id:
                open_db_orders = await db.get_open_orders_for_trade(self.trade_id)
                if open_db_orders:
                    exchange_orders = await ex.get_open_orders(self.client, self.symbol)
                    exchange_order_ids = {str(o["orderId"]) for o in exchange_orders}

                    for db_o in open_db_orders:
                        b_id = str(db_o["binance_order_id"])
                        if db_o["order_type"] == "LIMIT" and b_id not in exchange_order_ids:
                            # Order is no longer open -> inspect fill
                            ord_info = await ex.get_order(self.client, self.symbol, b_id)
                            if ord_info.get("status") == "FILLED":
                                fill_p = float(ord_info.get("avgPrice") or ord_info.get("price") or db_o["price"])
                                fill_q = float(ord_info.get("executedQty") or db_o["qty"])
                                logger.info("{}: REST poll detected filled limit order {}", self.symbol, b_id)
                                await db.update_order_status(b_id, "FILLED", datetime.now(timezone.utc).isoformat())
                                await self._on_limit_filled(b_id, fill_p, fill_q)

        except Exception as e:
            logger.warning("{}: REST poll error: {}", self.symbol, e)

    async def _handle_ws_message(self, msg: dict):
        """Process WS messages from the user-data stream."""
        event = msg.get("e", "")

        # 1. Position closed via ACCOUNT_UPDATE (TP fired)
        if event == "ACCOUNT_UPDATE":
            acc_data = msg.get("a", {})
            for p in acc_data.get("P", []):
                if p.get("s") == self.symbol and float(p.get("pa", 0)) == 0:
                    if self.filled_orders and not self._tp_hit:
                        logger.info("{}: Position closed via ACCOUNT_UPDATE (TP hit)", self.symbol)
                        await self._on_tp_filled(self.tp_price, self.total_qty)
                        return

        # 2. Algo execution report
        if event in ("ALGO_EXECUTION_REPORT", "algo_execution_report"):
            sym = msg.get("s") or msg.get("o", {}).get("s")
            status = msg.get("X") or msg.get("algoStatus") or msg.get("o", {}).get("algoStatus")
            if sym == self.symbol and status == "FILLED":
                if not self._tp_hit:
                    logger.info("{}: TP Algo order executed (FILLED)", self.symbol)
                    await self._on_tp_filled(self.tp_price, self.total_qty)
                    return

        # 3. Standard ORDER_TRADE_UPDATE
        if event != "ORDER_TRADE_UPDATE":
            return

        order_data   = msg.get("o", {})
        sym          = order_data.get("s", "")
        if sym != self.symbol:
            return

        order_status = order_data.get("X", "")   # NEW, FILLED, CANCELED, etc.
        order_id_ws  = str(order_data.get("i", ""))
        order_type   = (order_data.get("o", "") or order_data.get("ot", "")).upper()
        fill_price   = float(order_data.get("ap", 0) or order_data.get("L", 0) or order_data.get("p", 0) or 0)
        fill_qty     = float(order_data.get("z", 0) or order_data.get("l", 0) or 0)

        logger.debug("{}: WS order event id={} type={} status={} price={:.6g} qty={:.4f}",
                     self.symbol, order_id_ws, order_type, order_status, fill_price, fill_qty)

        if order_status == "FILLED":
            await db.update_order_status(
                order_id_ws, "FILLED", datetime.now(timezone.utc).isoformat()
            )
            await db.log_event(
                self.trade_id, self.symbol, "ORDER_FILLED",
                json.dumps({"order_id": order_id_ws, "type": order_type,
                            "price": fill_price, "qty": fill_qty})
            )

            if "TAKE_PROFIT" in order_type or order_id_ws == self.tp_order_id:
                if not self._tp_hit:
                    await self._on_tp_filled(fill_price or self.tp_price, fill_qty or self.total_qty)

            elif order_type == "LIMIT":
                already_processed = any(
                    str(o[2]) == order_id_ws or (abs(o[0] - fill_price) < 1e-8 and abs(o[1] - fill_qty) < 1e-8)
                    for o in self.filled_orders
                )
                if not already_processed:
                    await self._on_limit_filled(order_id_ws, fill_price, fill_qty)

    async def _on_limit_filled(self, order_id: str, fill_price: float, fill_qty: float):
        """Handle a grid limit order fill: update average entry, handle trailing TP, notify."""
        orders = await db.get_orders_for_trade(self.trade_id)
        order_index = next(
            (o["order_index"] for o in orders if str(o["binance_order_id"]) == str(order_id)),
            None
        )
        if order_index is None:
            order_index = len(self.filled_orders) + 1

        # Prevent duplicate processing
        if any(idx == order_index for _, _, idx in self.filled_orders):
            return

        self.filled_orders.append((fill_price, fill_qty, order_index))
        self.total_qty += fill_qty

        # Recompute average entry
        self.avg_entry = strat.compute_average_entry(
            [(p, q) for p, q, _ in self.filled_orders]
        )

        # Handle Trailing Take Profit configuration
        if self.trailing_take_profit:
            # Trailing TP: update tp_price relative to NEW avg_entry
            self.tp_price = strat.compute_tp_price(self.avg_entry, self.take_profit, self.direction)
            await self._place_take_profit()
        else:
            # Fixed TP: tp_price remains relative to initial_entry
            self.tp_price = strat.compute_tp_price(self.initial_entry, self.take_profit, self.direction)

        await db.update_trade(self.trade_id, entry_price=self.avg_entry, tp_price=self.tp_price)

        # Compute Telegram notification parameters
        # Reversal distance to TP from this executed limit order price:
        tp_pct = strat.tp_distance_pct(fill_price, self.tp_price, self.direction)

        total_deposit = sum(p * q for p, q, _ in self.filled_orders)

        expected_profit = strat.expected_profit_usdt(
            [(p, q) for p, q, _ in self.filled_orders],
            self.tp_price, self.direction
        )

        logger.info(
            "{}: limit {}/{} filled price={:.6g} avg={:.6g} tp={:.6g} (trailing={}) rev_dist={:.2f}% dep={:.2f} exp_pnl={:.2f}",
            self.symbol, order_index, self.orders_qty, fill_price, self.avg_entry,
            self.tp_price, self.trailing_take_profit, tp_pct, total_deposit, expected_profit
        )

        await self.notifier.limit_order_filled(
            symbol=self.symbol,
            order_index=order_index,
            orders_qty=self.orders_qty,
            fill_price=fill_price,
            avg_entry=self.avg_entry,
            tp_price=self.tp_price,
            tp_pct=tp_pct,
            total_deposit=total_deposit,
            expected_profit=expected_profit,
        )

    async def _on_tp_filled(self, fill_price: float, fill_qty: float):
        """Handle take-profit fill: cancel remaining orders, close DB trade, notify."""
        if self._tp_hit:
            return
        self._tp_hit = True

        logger.info("{}: TP filled at {:.6g}!", self.symbol, fill_price)

        # Cancel all remaining limit & algo orders instantly
        await ex.cancel_all_open_orders(self.client, self.symbol)

        # Mark remaining open orders as CANCELED in DB
        if self.trade_id:
            open_orders = await db.get_open_orders_for_trade(self.trade_id)
            for o in open_orders:
                await db.update_order_status(o["binance_order_id"], "CANCELED")

        # Compute realized PnL
        pnl = strat.expected_profit_usdt(
            [(p, q) for p, q, _ in self.filled_orders],
            fill_price, self.direction
        )

        # Close trade in DB
        if self.trade_id:
            await db.close_trade(self.trade_id, realized_pnl=pnl)

        # Get updated balance
        balance = await ex.get_futures_balance(self.client)

        await self.notifier.take_profit_hit(
            self.symbol, pnl, balance, self.try_number, self.max_tries
        )
        if self.trade_id:
            await db.log_event(
                self.trade_id, self.symbol, "TP_FILLED",
                json.dumps({"fill_price": fill_price, "pnl": pnl, "balance": balance})
            )

    def stop(self):
        """Signal the trader to stop after current cycle."""
        self._stop = True
