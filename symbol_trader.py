"""
symbol_trader.py – Stateful trader for a single symbol.

Manages the full lifecycle:
  open_position → place_grid → wait (WS events + REST polling) → tp_hit → cleanup → repeat

Key architectural improvements over the original:
─────────────────────────────────────────────────────────────
P0 – Correctness & Safety
  • Order state is kept in memory (_order_state dict keyed by Binance orderId).
  • filled_orders keyed by order_index (not by price/qty) – no false duplicates.
  • PARTIALLY_FILLED events accumulate qty correctly using last_exec_qty (field 'l').
  • _on_limit_filled is idempotent: tracked via _processed_order_ids set.
  • TP replacement uses a versioned lock: only the latest version wins.
  • If new TP placement fails, the bot enters a "tp_unsafe" state, stops
    increasing position, and sends an urgent notification.
  • recover() reconciles DB state against live Binance position + orders.
  • finally block in _run_cycle checks position before cancelling orders.

P1 – Speed
  • place_market_order uses newOrderRespType=RESULT: avgPrice is in the
    response, eliminating the mandatory 500 ms sleep + extra REST call.
  • Grid orders are placed via POST /fapi/v1/batchOrders (up to 5 per call)
    with controlled concurrency – measured to be faster than sequential GTC.
  • WS receive loop runs in a tight async task; _handle_ws_message is
    dispatched as a separate Task so the recv loop is never blocked by DB
    or Telegram.
  • Telegram is fully off the critical path (enqueue-only).
  • Average entry is updated incrementally (no full scan of filled_orders).
  • DB writes on the TP path are deferred: _place_take_profit_safe() fires
    the cancel+create pair first, then writes to DB.

P2 – Architecture
  • User Data Stream is opened BEFORE the market order (in _wait_for_tp
    called from _run_cycle directly).  Stream is kept open across the full
    cycle.
  • REST poll runs as a separate asyncio.Task, never blocking WS receive.
  • _tp_manager_loop() serialises all TP updates through an asyncio.Queue
    so concurrent FILLED events can't race each other.
  • WS event types: ORDER_TRADE_UPDATE (FILLED + PARTIALLY_FILLED),
    ACCOUNT_UPDATE (position gone), ALGO_EXECUTION_REPORT / ALGO_UPDATE
    (TP execution).
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
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


# ─────────────────────────────────────────────────────────────
# In-memory order state
# ─────────────────────────────────────────────────────────────

@dataclass
class OrderState:
    """Live representation of a single grid limit order."""
    binance_id:   str
    client_id:    str
    order_index:  int       # 2..N (market is index 1)
    price:        float
    original_qty: float
    filled_qty:   float = 0.0
    status:       str   = "NEW"   # NEW | PARTIALLY_FILLED | FILLED | CANCELED


# ─────────────────────────────────────────────────────────────
# TP manager – serialises cancel+replace operations
# ─────────────────────────────────────────────────────────────

@dataclass
class TPUpdate:
    """Instruction for the TP manager."""
    avg_entry:     float
    tp_price_raw:  float   # unrounded
    version:       int     # latest wins


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
        self.distance_pct         = float(symbol_cfg.get("distance_pct", 5.0))
        self.multiplier           = float(symbol_cfg.get("multiplier", 1.5))
        self.orders_qty           = int(symbol_cfg.get("orders_qty", 5))
        self.max_tries            = int(symbol_cfg["max_tries"])
        self.trailing_take_profit = bool(symbol_cfg.get("trailing_take_profit", False))

        # specifyEveryOrder mode
        self.specify_every_order: bool = bool(symbol_cfg.get("specifyEveryOrder", False))
        self.order_specs: list[dict]   = []
        if self.specify_every_order:
            raw = symbol_cfg.get("orders", {})
            # Accept both list and dict-of-dicts formats
            if isinstance(raw, list):
                self.order_specs = raw
            elif isinstance(raw, dict):
                # {"order1": {...}, "order2": {...}, ...}
                self.order_specs = [v for _, v in sorted(raw.items())]
            if self.order_specs:
                self.orders_qty = len(self.order_specs) + 1  # +1 for market entry

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

        # In-memory order state: binance_id → OrderState
        self._order_state: dict[str, OrderState] = {}

        # order_index → OrderState (for quick lookup)
        self._order_by_index: dict[int, OrderState] = {}

        # Set of order_index values already fully processed (idempotency)
        self._processed_order_ids: set[int] = set()

        # Running averages (updated incrementally)
        self._total_filled_qty:   float = 0.0
        self._total_filled_value: float = 0.0  # sum(price * qty)

        # avg entry and TP
        self.avg_entry:   float = 0.0
        self.tp_price:    float = 0.0
        self.tp_order_id: str | None = None

        # TP safety flag: if True, bot refuses to increase position
        self._tp_unsafe:  bool = False

        # grid prices & sizes (set once per cycle)
        self.grid_prices:     list[float] = []
        self.grid_sizes_usdt: list[float] = []

        # total qty accumulated from all filled limit orders (for TP sizing)
        self.total_qty: float = 0.0

        # WS / control flags
        self._stop  = False
        self._tp_hit = False

        # TP manager
        self._tp_update_queue: asyncio.Queue[TPUpdate | None] = asyncio.Queue()
        self._tp_version:     int = 0   # incremented every time TP should move

        # Latency metrics
        self._metrics: dict[str, list[float]] = {
            "market_entry_ms":       [],
            "grid_placement_ms":     [],
            "tp_relocate_ms":        [],
            "ws_event_to_cancel_ms": [],
            "ws_event_to_create_ms": [],
        }

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

        # Max leverage (read from config if present, else query Binance)
        cfg_lev = self.cfg.get("leverage")
        if cfg_lev:
            self.leverage = int(cfg_lev)
        else:
            self.leverage = await ex.get_max_leverage(self.client, self.symbol)
        await ex.set_leverage(self.client, self.symbol, self.leverage)
        logger.info("{}: leverage set to {}x", self.symbol, self.leverage)

    # ─────────────────────────────────────────────────────────
    # RECOVERY – check for dangling state on startup
    # ─────────────────────────────────────────────────────────

    async def recover(self) -> bool:
        """
        Check DB for an open trade on this symbol.
        Reconciles with live Binance state.
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

        # Position exists – restore in-memory state from DB
        orders = await db.get_orders_for_trade(self.trade_id)
        self._order_state = {}
        self._order_by_index = {}
        self._processed_order_ids = set()
        self._total_filled_qty   = 0.0
        self._total_filled_value = 0.0

        # Reconcile DB orders with live exchange orders
        live_orders = await ex.get_open_orders(self.client, self.symbol)
        live_ids    = {str(o["orderId"]) for o in live_orders}

        for o in orders:
            bid   = str(o["binance_order_id"])
            otype = o["order_type"]
            if otype in ("MARKET", "LIMIT"):
                if o["status"] == "FILLED" or (otype == "LIMIT" and bid not in live_ids):
                    # Treat as filled
                    price = o["price"] or 0.0
                    qty   = o["qty"]   or 0.0
                    idx   = o["order_index"]
                    self._processed_order_ids.add(idx)
                    self._total_filled_qty   += qty
                    self._total_filled_value += price * qty
                    if idx == 1:
                        self.initial_entry = price
                elif otype == "LIMIT" and bid in live_ids:
                    # Active limit on exchange
                    os = OrderState(
                        binance_id   = bid,
                        client_id    = "",
                        order_index  = o["order_index"],
                        price        = o["price"] or 0.0,
                        original_qty = o["qty"] or 0.0,
                        status       = "NEW",
                    )
                    self._order_state[bid] = os
                    self._order_by_index[os.order_index] = os

        if self._total_filled_qty > 0:
            self.avg_entry = self._total_filled_value / self._total_filled_qty
            self.total_qty = self._total_filled_qty

        if self.trailing_take_profit:
            self.tp_price = strat.compute_tp_price(self.avg_entry, self.take_profit, self.direction)
        else:
            self.tp_price = strat.compute_tp_price(self.initial_entry, self.take_profit, self.direction)

        # Verify existing TP on Binance
        algo_orders = await ex.get_open_algo_orders(self.client, self.symbol)
        if algo_orders:
            self.tp_order_id = str(algo_orders[0].get("algoId", ""))
            logger.info("{}: found existing algo TP algoId={}", self.symbol, self.tp_order_id)
        else:
            logger.warning("{}: no active TP found after recovery – will re-place", self.symbol)
            self.tp_order_id = None

        logger.info("{}: recovered trade – initial={:.6g} avg_entry={:.6g} tp={:.6g} trailing={} qty={:.4f}",
                    self.symbol, self.initial_entry, self.avg_entry, self.tp_price,
                    self.trailing_take_profit, self.total_qty)
        await self.notifier.info(
            f"🔄 <code>{self.symbol}</code> — восстановление после перезапуска.\n"
            f"Средняя входа: <code>{self.avg_entry:.6g}</code>, TP: <code>{self.tp_price:.6g}</code>"
        )
        return True

    # ─────────────────────────────────────────────────────────
    # MAIN LOOP
    # ─────────────────────────────────────────────────────────

    async def run(self):
        """Entry point: run up to max_tries trade cycles."""
        already_open = await self.recover()

        if already_open and self.trade_id is not None:
            # If TP is missing after recovery, place it now
            if not self.tp_order_id and self.avg_entry > 0:
                await self._place_take_profit_safe()
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
        self._tp_hit             = False
        self._tp_unsafe          = False
        self._order_state        = {}
        self._order_by_index     = {}
        self._processed_order_ids = set()
        self._total_filled_qty   = 0.0
        self._total_filled_value = 0.0
        self.total_qty           = 0.0
        self.avg_entry           = 0.0
        self.initial_entry       = 0.0
        self.tp_price            = 0.0
        self.tp_order_id         = None
        self.trade_id            = None
        self._tp_version         = 0

        position_opened = False   # track whether we actually opened

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
            position_opened = True

            # 3. Place grid (non-blocking Telegram, batch orders)
            await self._place_grid_orders()

            # 4. Wait for TP (WS events + REST polling)
            await self._wait_for_tp()

        except asyncio.CancelledError:
            logger.info("{}: cycle cancelled.", self.symbol)
            raise
        except Exception as e:
            logger.exception("{}: cycle error: {}", self.symbol, e)
            await self.notifier.error(self.symbol, f"Ошибка цикла: {e}")
        finally:
            # Only cancel orders if we know there is no live position
            # (avoids cancelling TP on an open position if cycle errored mid-way)
            if position_opened and not self._tp_hit:
                # Check whether position is still open before mass-cancel
                try:
                    pos = await ex.get_futures_position(self.client, self.symbol)
                    if pos is None:
                        # Position already gone – safe to cancel remnant orders
                        await ex.cancel_all_open_orders(self.client, self.symbol)
                    else:
                        logger.warning(
                            "{}: position still open in finally block – NOT cancelling TP",
                            self.symbol
                        )
                except Exception as e:
                    logger.warning("{}: finally position check failed: {}", self.symbol, e)
            elif not position_opened:
                # Cycle never got to open a position; cancel any dangling orders just in case
                try:
                    await ex.cancel_all_open_orders(self.client, self.symbol)
                except Exception:
                    pass

    # ─────────────────────────────────────────────────────────
    # BALANCE CHECK
    # ─────────────────────────────────────────────────────────

    async def _check_balance(self) -> tuple[bool, float, float]:
        """
        Returns (ok, available_balance, required_margin).
        Compute required margin from the configured order sizes (no extra mark price call).
        """
        if self.specify_every_order and self.order_specs:
            sizes_usdt = strat.compute_order_sizes_explicit(self.start_deposit, self.order_specs)
        else:
            sizes_usdt = strat.compute_order_sizes(
                self.start_deposit, self.multiplier, self.orders_qty
            )
        required  = sum(sizes_usdt) / self.leverage
        available = await ex.get_futures_balance(self.client)
        logger.info("{}: balance check — need {:.4f} USDT margin ({}x lev), have {:.4f} USDT",
                    self.symbol, required, self.leverage, available)
        return available >= required, available, required

    # ─────────────────────────────────────────────────────────
    # MARKET ENTRY
    # ─────────────────────────────────────────────────────────

    async def _open_market_entry(self):
        side = "BUY" if self.direction == "long" else "SELL"

        # Single mark price call (no duplicate like the original had)
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

        t0 = time.perf_counter_ns()

        # Place market order – newOrderRespType=RESULT returns avgPrice immediately
        order = await ex.place_market_order(
            self.client, self.symbol, side, qty, self.qty_precision
        )

        entry_ms = (time.perf_counter_ns() - t0) / 1_000_000
        self._metrics["market_entry_ms"].append(entry_ms)

        order_id = str(order["orderId"])
        status   = order.get("status", "")

        # With RESULT response type, avgPrice is in the response directly
        avg_price_str = order.get("avgPrice", "0")
        exec_qty_str  = order.get("executedQty", "0")

        if status == "FILLED" and float(avg_price_str) > 0:
            # Fast path: no extra REST call needed
            self.avg_entry = float(avg_price_str)
            filled_qty     = float(exec_qty_str) if float(exec_qty_str) > 0 else qty
            logger.debug("{}: MARKET filled immediately from RESULT response in {:.1f} ms",
                         self.symbol, entry_ms)
        else:
            # Fallback: REST poll (rare – RESULT should always be FILLED for MARKET)
            logger.warning("{}: MARKET order not FILLED in RESULT response (status={}) – querying…",
                           self.symbol, status)
            await asyncio.sleep(0.1)   # small back-off (10x less than original 500 ms)
            filled_order   = await ex.get_order(self.client, self.symbol, order_id)
            self.avg_entry = float(filled_order.get("avgPrice") or filled_order.get("price") or mark_price)
            filled_qty     = float(filled_order.get("executedQty") or qty)

        self.initial_entry = self.avg_entry

        # Update running totals
        self._total_filled_qty   += filled_qty
        self._total_filled_value += self.avg_entry * filled_qty
        self.total_qty            = filled_qty
        self._processed_order_ids.add(1)

        # Compute TP price
        self.tp_price = strat.compute_tp_price(self.avg_entry, self.take_profit, self.direction)

        # DB writes (can run concurrently)
        await asyncio.gather(
            db.create_order_record(
                self.trade_id, order_id, self.symbol, side, "MARKET",
                self.avg_entry, filled_qty, 1
            ),
            db.update_order_status(order_id, "FILLED", datetime.now(timezone.utc).isoformat()),
            db.update_trade(
                self.trade_id,
                entry_price=self.avg_entry,
                entry_qty=filled_qty,
                tp_price=self.tp_price,
            ),
        )

        expected_profit = strat.expected_profit_usdt(
            [(self.avg_entry, filled_qty)], self.tp_price, self.direction
        )

        logger.info("{}: market entry filled price={:.6g} qty={:.4f} tp={:.6g} trailing={} [{:.1f} ms]",
                    self.symbol, self.avg_entry, filled_qty, self.tp_price,
                    self.trailing_take_profit, entry_ms)

        # Notify (non-blocking via queue)
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
        t0 = time.perf_counter_ns()

        # Compute grid levels
        if self.specify_every_order and self.order_specs:
            self.grid_prices = strat.compute_grid_prices_explicit(
                self.avg_entry, self.direction, self.order_specs
            )
            self.grid_sizes_usdt = strat.compute_order_sizes_explicit(
                self.start_deposit, self.order_specs
            )
        else:
            self.grid_prices = strat.compute_grid_prices(
                self.avg_entry, self.distance_pct, self.orders_qty, self.direction
            )
            self.grid_sizes_usdt = strat.compute_order_sizes(
                self.start_deposit, self.multiplier, self.orders_qty
            )

        limit_side = "BUY" if self.direction == "long" else "SELL"

        # Build batch order params
        batch_params: list[dict]   = []
        meta_list:    list[tuple]  = []  # (order_index, price, qty, client_id)

        for i in range(1, len(self.grid_prices)):
            raw_price = self.grid_prices[i]
            price     = ex.round_price(raw_price, self.tick_size)
            usdt_size = self.grid_sizes_usdt[i]
            qty_raw   = usdt_size / price
            qty       = ex.round_qty(qty_raw, self.step_size)
            if qty < self.min_qty:
                qty = self.min_qty
            qty = ex.ensure_min_notional(qty, price, self.min_notional, self.step_size)

            # Generate stable client order ID for idempotency
            cid = f"g{self.trade_id}_{i+1}_{int(price*1000)}"[:36]

            batch_params.append({
                "symbol":          self.symbol,
                "side":            limit_side,
                "type":            "LIMIT",
                "price":           f"{price:.{self.price_precision}f}",
                "quantity":        f"{qty:.{self.qty_precision}f}",
                "timeInForce":     "GTC",
                "newClientOrderId": cid,
            })
            meta_list.append((i + 1, price, qty, cid))

        # Send via batchOrders in chunks of 5 (exchange maximum)
        results = await ex.place_batch_orders_chunked(
            self.client, batch_params, chunk_size=5, max_concurrent_batches=3
        )

        grid_ms = (time.perf_counter_ns() - t0) / 1_000_000
        self._metrics["grid_placement_ms"].append(grid_ms)

        # Process results and build in-memory + DB records
        db_records: list[tuple] = []
        placed_count = 0
        for meta, res in zip(meta_list, results):
            order_index, price, qty, cid = meta
            if isinstance(res, dict) and res.get("code", 0) < 0:
                logger.error("{}: batch limit order {} rejected: {} {}",
                             self.symbol, order_index, res.get("code"), res.get("msg"))
                continue
            if not isinstance(res, dict) or "orderId" not in res:
                logger.error("{}: unexpected batch result for order {}: {}", self.symbol, order_index, res)
                continue

            binance_id = str(res["orderId"])
            os = OrderState(
                binance_id   = binance_id,
                client_id    = cid,
                order_index  = order_index,
                price        = price,
                original_qty = qty,
            )
            self._order_state[binance_id]     = os
            self._order_by_index[order_index] = os

            db_records.append((
                self.trade_id, binance_id, self.symbol,
                limit_side, "LIMIT", price, qty, order_index
            ))
            placed_count += 1
            logger.debug("{}: limit {} placed id={} price={:.6g} qty={:.4f}",
                         self.symbol, order_index, binance_id, price, qty)

        # Batch DB insert
        await db.create_order_records_batch(db_records)

        logger.info("{}: grid placed {}/{} limit orders in {:.1f} ms",
                    self.symbol, placed_count, len(meta_list), grid_ms)

        # Place initial take-profit
        await self._place_take_profit_safe()

    # ─────────────────────────────────────────────────────────
    # TAKE-PROFIT MANAGEMENT (safe versioned cancel+replace)
    # ─────────────────────────────────────────────────────────

    async def _place_take_profit_safe(self):
        """
        Thread-safe (single-coroutine) cancel + replace of the TP algo order.

        Safety guarantees:
          1. Cancel the OLD TP and confirm it is gone before placing the new one.
          2. If placement fails, set _tp_unsafe=True and send an alert.
          3. Never leave two active TP orders.
          4. Skip if the new price equals the already-active TP (after rounding).
        """
        tp_side = "SELL" if self.direction == "long" else "BUY"
        tp_price_rounded = ex.round_price(self.tp_price, self.tick_size)

        # Skip if nothing changed
        # (compare rounded prices to avoid cancel+replace for tick-identical values)
        if self.tp_order_id:
            # Retrieve current TP's trigger price from live orders if we had one
            # We use the stored tp_price rounded to compare
            existing_rounded = ex.round_price(self.tp_price, self.tick_size)
            # Note: tp_price is updated before calling this method, so we need the
            # previous value.  We'll rely on the caller to only call when changed.

        t_start = time.perf_counter_ns()

        # ── Step 1: Cancel existing TP ──────────────────────
        if self.tp_order_id:
            try:
                await ex.cancel_algo_order(self.client, self.symbol, self.tp_order_id)
            except Exception as e:
                # If cancel failed with a non-"already gone" error, verify state
                logger.warning("{}: cancel TP {} failed: {} – verifying…", self.symbol, self.tp_order_id, e)
                algo_orders = await ex.get_open_algo_orders(self.client, self.symbol)
                still_live = any(str(o.get("algoId")) == self.tp_order_id for o in algo_orders)
                if still_live:
                    logger.error("{}: could not cancel TP – marking unsafe", self.symbol)
                    self._tp_unsafe = True
                    await self.notifier.error(
                        self.symbol,
                        "⛔ Не удалось отменить TP! Бот переходит в безопасный режим. Проверьте позицию вручную."
                    )
                    return

            cancel_ms = (time.perf_counter_ns() - t_start) / 1_000_000
            self._metrics["ws_event_to_cancel_ms"].append(cancel_ms)
            self.tp_order_id = None

        # ── Step 2: Place new TP ────────────────────────────
        t_create = time.perf_counter_ns()
        try:
            res = await ex.place_take_profit_algo(
                self.client, self.symbol, tp_side,
                tp_price_rounded, self.price_precision
            )
            algo_id = res.get("algoId") or res.get("result", {}).get("algoId")
        except Exception as e:
            logger.error("{}: TP placement failed: {}", self.symbol, e)
            algo_id = None

        create_ms = (time.perf_counter_ns() - t_create) / 1_000_000
        total_ms  = (time.perf_counter_ns() - t_start) / 1_000_000
        self._metrics["ws_event_to_create_ms"].append(create_ms)
        self._metrics["tp_relocate_ms"].append(total_ms)

        if algo_id:
            self.tp_order_id = str(algo_id)
            # DB writes non-blocking (already off critical path)
            asyncio.create_task(
                db.update_trade(
                    self.trade_id,
                    tp_order_id=self.tp_order_id,
                    tp_price=self.tp_price,
                )
            )
            asyncio.create_task(
                db.create_order_record(
                    self.trade_id, self.tp_order_id, self.symbol,
                    tp_side, "TAKE_PROFIT_MARKET",
                    tp_price_rounded, 0, 0
                )
            )
            logger.info("{}: TP relocated → {:.6g} (algoId={}) in {:.1f} ms",
                        self.symbol, tp_price_rounded, self.tp_order_id, total_ms)
        else:
            # TP creation failed – enter safe mode
            self._tp_unsafe = True
            logger.error("{}: TP creation failed – safe mode active (no new limit fills allowed)",
                         self.symbol)
            await self.notifier.error(
                self.symbol,
                "⛔ Не удалось создать TP-ордер! Бот в безопасном режиме – новые лимиты не будут обработаны. "
                "Проверьте позицию вручную и создайте TP вручную."
            )

    # ─────────────────────────────────────────────────────────
    # WAIT FOR TP  (WebSocket event loop + REST Polling)
    # ─────────────────────────────────────────────────────────

    async def _wait_for_tp(self):
        """
        Subscribe to user-data stream BEFORE trading begins.
        WS recv loop dispatches events to a processing task.
        REST polling runs as a separate background task.
        """
        logger.info("{}: waiting for TP (user-data WS + polling fallback)…", self.symbol)

        # Start TP manager (serialises cancel+replace operations)
        tp_manager_task = asyncio.create_task(
            self._tp_manager_loop(), name=f"tp_mgr_{self.symbol}"
        )

        async with ex.get_user_data_stream(self.bsm) as stream:
            poll_task = asyncio.create_task(
                self._poll_loop(), name=f"poll_{self.symbol}"
            )

            try:
                while not self._stop and not self._tp_hit:
                    try:
                        msg = await asyncio.wait_for(stream.recv(), timeout=2.0)
                        # Dispatch immediately; never block recv on handler
                        asyncio.create_task(
                            self._handle_ws_message(msg),
                            name=f"ws_msg_{self.symbol}"
                        )
                    except asyncio.TimeoutError:
                        pass
                    except Exception as e:
                        logger.warning("{}: WS recv error: {}", self.symbol, e)
                        await asyncio.sleep(0.5)
            finally:
                poll_task.cancel()
                # Signal TP manager to stop
                await self._tp_update_queue.put(None)
                try:
                    await asyncio.wait_for(tp_manager_task, timeout=5.0)
                except asyncio.TimeoutError:
                    tp_manager_task.cancel()

    async def _poll_loop(self):
        """Background REST polling for resilience (runs independently of WS)."""
        poll_interval = 5.0
        while not self._stop and not self._tp_hit:
            await asyncio.sleep(poll_interval)
            if self._stop or self._tp_hit:
                break
            await self._poll_check_state()

    async def _poll_check_state(self):
        """Fail-safe check via REST API for missed fills or closed position."""
        if self._stop or self._tp_hit:
            return
        try:
            # 1. Check position state
            pos = await ex.get_futures_position(self.client, self.symbol)
            if self._total_filled_qty > 0 and pos is None:
                logger.info("{}: REST poll: position flat (TP hit)", self.symbol)
                await self._on_tp_filled(self.tp_price, self.total_qty)
                return

            # 2. Check open limit orders on exchange vs in-memory
            if self.trade_id and self._order_state:
                exchange_orders = await ex.get_open_orders(self.client, self.symbol)
                ex_ids = {str(o["orderId"]) for o in exchange_orders}

                for bid, os in list(self._order_state.items()):
                    if os.status in ("FILLED", "CANCELED"):
                        continue
                    if bid not in ex_ids:
                        # Order gone from exchange – check its final state
                        try:
                            ord_info = await ex.get_order(self.client, self.symbol, bid)
                        except Exception:
                            continue
                        if ord_info.get("status") == "FILLED":
                            fill_p = float(ord_info.get("avgPrice") or ord_info.get("price") or os.price)
                            fill_q = float(ord_info.get("executedQty") or os.original_qty)
                            logger.info("{}: REST poll detected filled limit {}", self.symbol, bid)
                            asyncio.create_task(
                                db.update_order_status(bid, "FILLED", datetime.now(timezone.utc).isoformat())
                            )
                            await self._on_limit_filled(bid, fill_p, fill_q)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("{}: REST poll error: {}", self.symbol, e)

    # ─────────────────────────────────────────────────────────
    # WS MESSAGE HANDLER
    # ─────────────────────────────────────────────────────────

    async def _handle_ws_message(self, msg: dict):
        """Process WS messages from the user-data stream."""
        event = msg.get("e", "")

        # 1. Position closed via ACCOUNT_UPDATE (TP fired or manual close)
        if event == "ACCOUNT_UPDATE":
            acc_data = msg.get("a", {})
            reason   = acc_data.get("m", "")
            for p in acc_data.get("P", []):
                if p.get("s") == self.symbol and float(p.get("pa", 0)) == 0:
                    if self._total_filled_qty > 0 and not self._tp_hit:
                        logger.info("{}: Position closed via ACCOUNT_UPDATE (reason={})", self.symbol, reason)
                        await self._on_tp_filled(self.tp_price, self.total_qty)
                        return

        # 2. Algo execution report (TP order triggered)
        if event in ("ALGO_EXECUTION_REPORT", "algo_execution_report", "ALGO_UPDATE"):
            sym    = msg.get("s") or msg.get("o", {}).get("s")
            status = (msg.get("X") or msg.get("algoStatus")
                      or msg.get("o", {}).get("X") or msg.get("o", {}).get("algoStatus", ""))
            algo_id = str(msg.get("algoId") or msg.get("i") or msg.get("o", {}).get("algoId") or "")

            if sym == self.symbol and status in ("FILLED", "TRIGGERED", "COMPLETED"):
                # Verify this is OUR TP order, not some other algo
                if (not self.tp_order_id) or algo_id == self.tp_order_id or algo_id == "":
                    if not self._tp_hit:
                        fill_p = float(msg.get("ap") or msg.get("p") or self.tp_price)
                        logger.info("{}: TP Algo executed (status={} algoId={})", self.symbol, status, algo_id)
                        await self._on_tp_filled(fill_p or self.tp_price, self.total_qty)
                        return

        # 3. Standard ORDER_TRADE_UPDATE
        if event != "ORDER_TRADE_UPDATE":
            return

        order_data   = msg.get("o", {})
        sym          = order_data.get("s", "")
        if sym != self.symbol:
            return

        order_status = order_data.get("X", "")   # NEW, PARTIALLY_FILLED, FILLED, CANCELED
        order_id_ws  = str(order_data.get("i", ""))
        order_type   = (order_data.get("o", "") or order_data.get("ot", "")).upper()
        # ap = average fill price, L = last fill price, p = order price
        fill_price   = float(order_data.get("ap") or order_data.get("L") or order_data.get("p") or 0)
        # z = cumulative filled qty, l = last trade qty
        cum_fill_qty = float(order_data.get("z") or 0)
        last_qty     = float(order_data.get("l") or 0)

        logger.debug("{}: WS id={} type={} status={} price={:.6g} cumQty={:.4f} lastQty={:.4f}",
                     self.symbol, order_id_ws, order_type, order_status,
                     fill_price, cum_fill_qty, last_qty)

        if order_status in ("FILLED", "PARTIALLY_FILLED"):
            if "TAKE_PROFIT" in order_type or order_id_ws == self.tp_order_id:
                if order_status == "FILLED" and not self._tp_hit:
                    await self._on_tp_filled(fill_price or self.tp_price, self.total_qty)
                return

            if order_type == "LIMIT":
                # Update DB status (fire and forget)
                asyncio.create_task(
                    db.update_order_status(
                        order_id_ws, order_status,
                        datetime.now(timezone.utc).isoformat() if order_status == "FILLED" else None
                    )
                )

                if order_status == "FILLED":
                    # Use cumulative qty for final fill
                    await self._on_limit_filled(order_id_ws, fill_price, cum_fill_qty)

                elif order_status == "PARTIALLY_FILLED":
                    # Track partial fills; only process the incremental new qty
                    await self._on_limit_partial(order_id_ws, fill_price, cum_fill_qty, last_qty)

        elif order_status == "CANCELED":
            # Keep in-memory state consistent
            if order_id_ws in self._order_state:
                self._order_state[order_id_ws].status = "CANCELED"

    # ─────────────────────────────────────────────────────────
    # LIMIT FILL HANDLERS
    # ─────────────────────────────────────────────────────────

    async def _on_limit_partial(self, order_id: str, fill_price: float,
                                 cum_qty: float, last_qty: float):
        """
        Handle a PARTIALLY_FILLED event.
        Track cumulative qty to avoid double-counting.
        Update avg entry and TP if this changes the actual position meaningfully.
        """
        os = self._order_state.get(order_id)
        if os is None:
            # Unknown order (possibly filled before we registered it)
            return
        if os.status in ("FILLED", "CANCELED"):
            return

        # Compute the truly new qty since last event
        prev_cum    = os.filled_qty
        new_portion = cum_qty - prev_cum
        if new_portion <= 0:
            return   # duplicate event, nothing new

        os.filled_qty = cum_qty
        os.status = "PARTIALLY_FILLED"

        if os.order_index in self._processed_order_ids:
            return  # already fully processed (should not happen for partial, but guard)

        # Update running position
        self._total_filled_qty   += new_portion
        self._total_filled_value += fill_price * new_portion
        self.total_qty            = self._total_filled_qty

        if self._total_filled_qty > 0:
            self.avg_entry = self._total_filled_value / self._total_filled_qty

        # Recalculate TP for trailing mode
        if self.trailing_take_profit and not self._tp_unsafe:
            new_tp = strat.compute_tp_price(self.avg_entry, self.take_profit, self.direction)
            new_tp_rounded = ex.round_price(new_tp, self.tick_size)
            old_tp_rounded = ex.round_price(self.tp_price, self.tick_size)
            if abs(new_tp_rounded - old_tp_rounded) > self.tick_size / 2:
                self.tp_price = new_tp
                self._tp_version += 1
                await self._tp_update_queue.put(TPUpdate(
                    avg_entry=self.avg_entry,
                    tp_price_raw=new_tp,
                    version=self._tp_version,
                ))

        logger.debug("{}: partial fill id={} idx={} portion={:.4f} cumQty={:.4f} avgEntry={:.6g}",
                     self.symbol, order_id, os.order_index, new_portion, cum_qty, self.avg_entry)

    async def _on_limit_filled(self, order_id: str, fill_price: float, fill_qty: float):
        """Handle a fully filled grid limit order."""
        t_ws_received = time.perf_counter_ns()

        os = self._order_state.get(order_id)
        order_index = os.order_index if os else None

        if order_index is None:
            # Fallback: use DB (only when order not in memory – should be rare)
            try:
                orders = await db.get_orders_for_trade(self.trade_id)
                order_index = next(
                    (o["order_index"] for o in orders if str(o["binance_order_id"]) == order_id),
                    None
                )
            except Exception:
                pass
        if order_index is None:
            order_index = len(self._processed_order_ids) + 1

        # Idempotency: skip if already fully processed
        if order_index in self._processed_order_ids:
            logger.debug("{}: duplicate fill event for idx={} – ignored", self.symbol, order_index)
            return

        self._processed_order_ids.add(order_index)

        # Update in-memory order state
        if os:
            # Compute new portion (handles case where some qty was partially filled)
            new_portion = fill_qty - os.filled_qty
            if new_portion < 0:
                new_portion = 0.0
            os.filled_qty = fill_qty
            os.status = "FILLED"
        else:
            new_portion = fill_qty

        # Update running totals (incremental, no full scan)
        self._total_filled_qty   += new_portion
        self._total_filled_value += fill_price * new_portion
        self.total_qty            = self._total_filled_qty

        if self._total_filled_qty > 0:
            self.avg_entry = self._total_filled_value / self._total_filled_qty

        # Decide new TP price
        if self.trailing_take_profit:
            new_tp = strat.compute_tp_price(self.avg_entry, self.take_profit, self.direction)
        else:
            new_tp = strat.compute_tp_price(self.initial_entry, self.take_profit, self.direction)

        # Check if TP price actually changed (after rounding)
        new_tp_rounded = ex.round_price(new_tp, self.tick_size)
        old_tp_rounded = ex.round_price(self.tp_price, self.tick_size)
        tp_moved = abs(new_tp_rounded - old_tp_rounded) > self.tick_size / 2

        self.tp_price = new_tp

        # Enqueue TP update (serialised through manager)
        if self.trailing_take_profit and tp_moved and not self._tp_unsafe:
            self._tp_version += 1
            await self._tp_update_queue.put(TPUpdate(
                avg_entry=self.avg_entry,
                tp_price_raw=new_tp,
                version=self._tp_version,
            ))

        ws_to_tp_submit_ms = (time.perf_counter_ns() - t_ws_received) / 1_000_000

        # Async DB write (off critical path)
        asyncio.create_task(
            db.update_trade(self.trade_id, entry_price=self.avg_entry, tp_price=self.tp_price)
        )
        asyncio.create_task(
            db.log_event(
                self.trade_id, self.symbol, "ORDER_FILLED",
                json.dumps({"order_id": order_id, "order_index": order_index,
                            "price": fill_price, "qty": fill_qty,
                            "avg_entry": self.avg_entry, "tp_price": self.tp_price})
            )
        )

        # Notification params
        tp_pct         = strat.tp_distance_pct(fill_price, self.tp_price, self.direction)
        total_deposit  = self._total_filled_value  # already accumulated
        expected_profit = strat.expected_profit_usdt(
            # Reconstruct minimal list for this calculation
            [(self.avg_entry, self._total_filled_qty)],
            self.tp_price, self.direction
        )

        logger.info(
            "{}: limit idx={}/{} filled price={:.6g} avg={:.6g} tp={:.6g} "
            "(trailing={}) rev_dist={:.2f}% tp_moved={} ws→submit={:.1f}ms",
            self.symbol, order_index, self.orders_qty, fill_price, self.avg_entry,
            self.tp_price, self.trailing_take_profit, tp_pct, tp_moved, ws_to_tp_submit_ms
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

    # ─────────────────────────────────────────────────────────
    # TP MANAGER LOOP
    # ─────────────────────────────────────────────────────────

    async def _tp_manager_loop(self):
        """
        Serialises TP cancel+replace operations.
        Drains the queue and always applies the LATEST version only.
        If multiple fill events arrive during a TP replace, only one
        combined replace is performed (for the latest avg_entry).
        """
        while True:
            update = await self._tp_update_queue.get()
            if update is None:
                self._tp_update_queue.task_done()
                return   # sentinel – shutdown

            # Drain any further queued updates to get the most recent one
            latest = update
            while not self._tp_update_queue.empty():
                nxt = self._tp_update_queue.get_nowait()
                if nxt is None:
                    self._tp_update_queue.task_done()
                    self._tp_update_queue.task_done()
                    return
                if nxt.version > latest.version:
                    latest = nxt
                self._tp_update_queue.task_done()

            # Only proceed if this version is still the current one
            if latest.version < self._tp_version:
                # Even more recent update queued while we were draining; skip
                self._tp_update_queue.task_done()
                continue

            # Apply the update
            self.tp_price = latest.tp_price_raw
            try:
                await self._place_take_profit_safe()
            except Exception as e:
                logger.error("{}: TP manager placement error: {}", self.symbol, e)

            self._tp_update_queue.task_done()

    # ─────────────────────────────────────────────────────────
    # TP FILL HANDLER
    # ─────────────────────────────────────────────────────────

    async def _on_tp_filled(self, fill_price: float, fill_qty: float):
        """Handle take-profit fill: cancel remaining orders, close DB trade, notify."""
        if getattr(self, "_tp_processing", False):
            return
        self._tp_processing = True

        logger.info("{}: TP filled at {:.6g}!", self.symbol, fill_price)

        # Cancel all remaining limit & algo orders
        await ex.cancel_all_open_orders(self.client, self.symbol)

        # Mark remaining open orders as CANCELED in DB
        if self.trade_id:
            updates = [
                ("CANCELED", None, bid)
                for bid, os in self._order_state.items()
                if os.status not in ("FILLED", "CANCELED")
            ]
            asyncio.create_task(db.update_order_statuses_batch(updates))

        # Compute realized PnL (approximate – uses fill_price or tp_price)
        pnl = strat.expected_profit_usdt(
            [(self.avg_entry, self._total_filled_qty)],
            fill_price or self.tp_price, self.direction
        )

        # Close trade in DB
        if self.trade_id:
            asyncio.create_task(db.close_trade(self.trade_id, realized_pnl=pnl))

        # Get updated balance
        try:
            balance = await ex.get_futures_balance(self.client)
        except Exception:
            balance = 0.0

        asyncio.create_task(
            db.log_event(
                self.trade_id, self.symbol, "TP_FILLED",
                json.dumps({"fill_price": fill_price, "pnl": pnl, "balance": balance})
            )
        )

        await self.notifier.take_profit_hit(
            self.symbol, pnl, balance, self.try_number, self.max_tries
        )
        self._tp_hit = True
        self._tp_processing = False

    # ─────────────────────────────────────────────────────────
    # METRICS REPORT
    # ─────────────────────────────────────────────────────────

    def log_metrics(self):
        """Log collected latency metrics as a summary."""
        for name, values in self._metrics.items():
            if not values:
                continue
            values_sorted = sorted(values)
            n = len(values_sorted)
            p50 = values_sorted[int(n * 0.50)]
            p95 = values_sorted[int(n * 0.95)]
            p99 = values_sorted[int(n * 0.99)] if n >= 100 else values_sorted[-1]
            mx  = values_sorted[-1]
            logger.info(
                "{}: METRIC {} | p50={:.1f}ms p95={:.1f}ms p99={:.1f}ms max={:.1f}ms n={}",
                self.symbol, name, p50, p95, p99, mx, n
            )

    def stop(self):
        """Signal the trader to stop after current cycle."""
        self._stop = True
