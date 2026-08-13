"""
exchange.py – Thin async wrapper around Binance UM Futures REST + WebSocket.

Uses python-binance's AsyncClient internally so every call is non-blocking.
"""

import asyncio
import math
from decimal import Decimal, ROUND_DOWN
from typing import Any

from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException
from loguru import logger


# ─────────────────────────────────────────────────────────────
# Client factory
# ─────────────────────────────────────────────────────────────
async def make_client(api_key: str, api_secret: str, testnet: bool) -> AsyncClient:
    client = await AsyncClient.create(
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
    )
    # Switch to futures endpoints if needed (handled automatically by python-binance)
    return client


# ─────────────────────────────────────────────────────────────
# Symbol meta
# ─────────────────────────────────────────────────────────────
async def get_symbol_info(client: AsyncClient, symbol: str) -> dict:
    """Returns exchange info filters for a futures symbol."""
    info = await client.futures_exchange_info()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            return s
    raise ValueError(f"Symbol {symbol} not found on Binance futures exchange")


def extract_filters(symbol_info: dict) -> dict:
    """Extract price precision, qty step, min notional from symbol_info filters."""
    result = {
        "price_precision": symbol_info.get("pricePrecision", 8),
        "qty_precision": symbol_info.get("quantityPrecision", 8),
        "tick_size": None,
        "step_size": None,
        "min_qty": None,
        "min_notional": None,
    }
    for f in symbol_info.get("filters", []):
        ft = f.get("filterType", "")
        if ft == "PRICE_FILTER":
            result["tick_size"] = float(f["tickSize"])
        elif ft == "LOT_SIZE":
            result["step_size"] = float(f["stepSize"])
            result["min_qty"] = float(f["minQty"])
        elif ft == "MIN_NOTIONAL":
            result["min_notional"] = float(f.get("notional", f.get("minNotional", 5)))
    return result


def round_price(price: float, tick_size: float) -> float:
    if tick_size is None or tick_size == 0:
        return price
    precision = max(0, round(-math.log10(tick_size)))
    return float(Decimal(str(price)).quantize(Decimal(str(tick_size)), rounding=ROUND_DOWN))


def round_qty(qty: float, step_size: float) -> float:
    if step_size is None or step_size == 0:
        return qty
    return float(Decimal(str(qty)).quantize(Decimal(str(step_size)), rounding=ROUND_DOWN))


def ensure_min_notional(qty: float, price: float,
                         min_notional: float, step_size: float) -> float:
    """
    If qty * price < min_notional, increment qty by one step_size at a time
    until the notional requirement is met.
    This handles the case where ROUND_DOWN brings the notional below the
    exchange minimum (Binance error -4164).
    """
    if price <= 0 or step_size is None or step_size == 0:
        return qty
    while qty * price < min_notional:
        qty = float(Decimal(str(qty)) + Decimal(str(step_size)))
    return qty


# ─────────────────────────────────────────────────────────────
# Account / margin
# ─────────────────────────────────────────────────────────────
async def get_max_leverage(client: AsyncClient, symbol: str) -> int:
    """Query brackets and return the highest available leverage tier."""
    try:
        brackets = await client.futures_leverage_bracket(symbol=symbol)
        if brackets:
            return int(brackets[0]["brackets"][0]["initialLeverage"])
    except Exception as e:
        logger.warning("Could not get max leverage for {}: {}", symbol, e)
    return 125  # fallback


async def set_leverage(client: AsyncClient, symbol: str, leverage: int) -> dict:
    return await client.futures_change_leverage(symbol=symbol, leverage=leverage)


async def set_cross_margin(client: AsyncClient, symbol: str) -> dict:
    try:
        return await client.futures_change_margin_type(symbol=symbol, marginType="CROSSED")
    except BinanceAPIException as e:
        if "No need to change margin type" in str(e):
            return {"msg": "already cross"}
        raise


async def get_futures_balance(client: AsyncClient) -> float:
    """Return available USDT balance on UM Futures account."""
    balances = await client.futures_account_balance()
    for b in balances:
        if b["asset"] == "USDT":
            return float(b["availableBalance"])
    return 0.0


async def get_futures_position(client: AsyncClient, symbol: str) -> dict | None:
    """Return current position dict for symbol, or None if flat."""
    positions = await client.futures_position_information(symbol=symbol)
    for p in positions:
        if p["symbol"] == symbol and float(p["positionAmt"]) != 0:
            return p
    return None


# ─────────────────────────────────────────────────────────────
# Order placement
# ─────────────────────────────────────────────────────────────
async def place_market_order(client: AsyncClient, symbol: str, side: str,
                              qty: float, qty_precision: int) -> dict:
    """Place a market order. side: 'BUY' | 'SELL'"""
    qty_str = f"{qty:.{qty_precision}f}"
    order = await client.futures_create_order(
        symbol=symbol,
        side=side,
        type="MARKET",
        quantity=qty_str,
    )
    logger.info("Market order placed: {} {} qty={}", side, symbol, qty_str)
    return order


async def place_limit_order(client: AsyncClient, symbol: str, side: str,
                             price: float, qty: float,
                             price_precision: int, qty_precision: int) -> dict:
    """Place a reduce-only or normal limit order."""
    price_str = f"{price:.{price_precision}f}"
    qty_str = f"{qty:.{qty_precision}f}"
    order = await client.futures_create_order(
        symbol=symbol,
        side=side,
        type="LIMIT",
        price=price_str,
        quantity=qty_str,
        timeInForce="GTC",
    )
    logger.info("Limit order placed: {} {} price={} qty={}", side, symbol, price_str, qty_str)
    return order


async def place_take_profit_algo(client: AsyncClient, symbol: str, side: str,
                              stop_price: float,
                              price_precision: int) -> dict:
    """
    Place a TAKE_PROFIT_MARKET order via Binance Algo Order API (POST /fapi/v1/algoOrder).
    Uses closePosition=true — closes entire position when triggerPrice is reached.
    side: opposite of position direction (long→SELL, short→BUY).
    """
    sp_str = f"{stop_price:.{price_precision}f}"
    params = {
        "algoType": "CONDITIONAL",
        "symbol": symbol,
        "side": side,
        "type": "TAKE_PROFIT_MARKET",
        "triggerPrice": sp_str,
        "closePosition": "true",
        "workingType": "CONTRACT_PRICE",
    }
    res = await client._request_futures_api("post", "algoOrder", True, data=params)
    logger.info("TP Algo order placed: {} {} triggerPrice={} (closePosition=true)", side, symbol, sp_str)
    return res


async def cancel_algo_order(client: AsyncClient, symbol: str, algo_id: str | int) -> dict:
    """Cancel a single Algo order using DELETE /fapi/v1/algoOrder."""
    try:
        res = await client._request_futures_api("delete", "algoOrder", True, data={"symbol": symbol, "algoId": str(algo_id)})
        logger.info("Cancelled algo order {} on {}", algo_id, symbol)
        return res
    except Exception as e:
        logger.warning("Cancel algo order {} on {} failed: {}", algo_id, symbol, e)
        return {}


async def cancel_all_algo_orders(client: AsyncClient, symbol: str):
    """Cancel all open Algo orders for symbol."""
    try:
        orders = await client._request_futures_api("get", "openAlgoOrders", True, data={"symbol": symbol})
        for o in orders:
            algo_id = o.get("algoId")
            if algo_id:
                await cancel_algo_order(client, symbol, algo_id)
    except Exception as e:
        logger.warning("cancel_all_algo_orders for {}: {}", symbol, e)


async def cancel_order(client: AsyncClient, symbol: str, order_id: str) -> dict:
    try:
        result = await client.futures_cancel_order(symbol=symbol, orderId=int(order_id))
        logger.info("Cancelled order {} on {}", order_id, symbol)
        return result
    except BinanceAPIException as e:
        if "Unknown order" in str(e) or "-2011" in str(e):
            logger.warning("Order {} already gone: {}", order_id, e)
            return {}
        raise


async def cancel_all_open_orders(client: AsyncClient, symbol: str):
    try:
        await client.futures_cancel_all_open_orders(symbol=symbol)
        logger.info("Cancelled all open limit orders for {}", symbol)
    except BinanceAPIException as e:
        logger.warning("cancel_all_open_orders {}: {}", symbol, e)
    await cancel_all_algo_orders(client, symbol)


async def get_order(client: AsyncClient, symbol: str, order_id: str) -> dict:
    return await client.futures_get_order(symbol=symbol, orderId=int(order_id))


async def get_open_orders(client: AsyncClient, symbol: str) -> list[dict]:
    return await client.futures_get_open_orders(symbol=symbol)


# ─────────────────────────────────────────────────────────────
# Price query
# ─────────────────────────────────────────────────────────────
async def get_mark_price(client: AsyncClient, symbol: str) -> float:
    data = await client.futures_mark_price(symbol=symbol)
    return float(data["markPrice"])


async def get_last_trade_price(client: AsyncClient, symbol: str) -> float:
    """Last traded price (more accurate than mark for entry confirmation)."""
    trades = await client.futures_recent_trades(symbol=symbol, limit=1)
    return float(trades[0]["price"])


# ─────────────────────────────────────────────────────────────
# Income / PnL
# ─────────────────────────────────────────────────────────────
async def get_income(client: AsyncClient, symbol: str, start_time: int) -> float:
    """Sum REALIZED_PNL income records after start_time (ms)."""
    records = await client.futures_income_history(
        symbol=symbol, incomeType="REALIZED_PNL", startTime=start_time, limit=100
    )
    return sum(float(r["income"]) for r in records)


# ─────────────────────────────────────────────────────────────
# WebSocket helpers  (return async context managers / streams)
# ─────────────────────────────────────────────────────────────
def get_user_data_stream(bsm: BinanceSocketManager):
    """Returns an async context manager for the futures user data stream."""
    return bsm.futures_user_socket()


def get_symbol_ticker_stream(bsm: BinanceSocketManager, symbol: str):
    """Mark price ticker stream."""
    return bsm.futures_symbol_mark_price_socket(symbol.lower())
