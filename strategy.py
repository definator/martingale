"""
strategy.py – Core martingale grid logic.

Responsible for:
  1. Computing order grid prices (harmonic spacing so gaps widen with distance)
  2. Computing order sizes (geometric progression with multiplier)
  3. Managing average entry price as grid orders fill
  4. Computing take-profit price from average entry
"""

from __future__ import annotations
import math


# ─────────────────────────────────────────────────────────────
# Grid price calculation
# ─────────────────────────────────────────────────────────────

def compute_grid_prices(
    start_price: float,
    distance_pct: float,
    orders_qty: int,
    direction: str,
) -> list[float]:
    """
    Return a list of `orders_qty` prices.

    Index 0 is the market entry price (start_price).
    Indices 1..N-1 are limit order prices spaced with harmonic intervals.

    For SHORT: limits are placed ABOVE start_price.
    For LONG: limits are placed BELOW start_price.

    Algorithm:
    1. First, calculate reference prices as if entering SHORT (upward harmonic grid):
       end_price = start_price * (1 + distance_pct / 100)
       first_num = 1 / start_price
       last_num  = 1 / end_price
       diff      = first_num - last_num
       coeff     = diff / (orders_qty - 1)
       ref_prices[k] = 1 / (first_num - coeff * k)  for k = 0..N-1

    2. Calculate percentage distance between consecutive reference prices:
       distance_pct_1 = (ref_prices[1] - ref_prices[0]) / ref_prices[0]
       distance_pct_2 = (ref_prices[2] - ref_prices[1]) / ref_prices[1]
       ...

    3. If SHORT: return ref_prices directly (placed above entry).

    4. If LONG: market order opens position at start_price (1st order).
       2nd order (limit buy) is placed distance_pct_1 % down from 1st order.
       3rd order (limit buy) is placed distance_pct_2 % down from 2nd order.
       And so on.
    """
    if orders_qty < 2:
        return [start_price]

    # Reference grid (always computed "upward" from start_price)
    end_price = start_price * (1.0 + distance_pct / 100.0)
    first_num = 1.0 / start_price
    last_num  = 1.0 / end_price
    diff      = first_num - last_num          # positive
    coeff     = diff / (orders_qty - 1)

    ref_prices = []
    for k in range(orders_qty):
        p = 1.0 / (first_num - coeff * k)
        ref_prices.append(p)

    if direction.lower() == "short":
        # Sell limits above entry → use reference prices directly
        return ref_prices

    else:  # long
        # Buy limits below entry → mirror percentage gaps downward
        pct_gaps = []
        for i in range(len(ref_prices) - 1):
            gap = (ref_prices[i + 1] - ref_prices[i]) / ref_prices[i]
            pct_gaps.append(gap)

        prices = [start_price]
        for gap in pct_gaps:
            next_p = prices[-1] * (1.0 - gap)
            prices.append(next_p)
        return prices


def compute_order_sizes(
    start_deposit: float,
    multiplier: float,
    orders_qty: int,
) -> list[float]:
    """
    Return list of order sizes in USDT notional for each grid level.

    sizes[0] = start_deposit  (market order)
    sizes[k] = sizes[k-1] * multiplier

    Caller divides each size by price to get contract qty.
    """
    sizes = [start_deposit]
    for _ in range(orders_qty - 1):
        sizes.append(sizes[-1] * multiplier)
    return sizes


# ─────────────────────────────────────────────────────────────
# Average entry tracking
# ─────────────────────────────────────────────────────────────

def compute_average_entry(
    filled_orders: list[tuple[float, float]]  # list of (price, qty)
) -> float:
    """Weighted average entry price from (price, qty) pairs."""
    total_qty   = sum(q for _, q in filled_orders)
    total_value = sum(p * q for p, q in filled_orders)
    if total_qty == 0:
        return 0.0
    return total_value / total_qty


def total_filled_qty(filled_orders: list[tuple[float, float]]) -> float:
    return sum(q for _, q in filled_orders)


# ─────────────────────────────────────────────────────────────
# Take-profit price
# ─────────────────────────────────────────────────────────────

def compute_tp_price(avg_entry: float, take_profit_pct: float, direction: str) -> float:
    """
    Return the price at which to set the take-profit stop.

    LONG : TP is above avg_entry by take_profit_pct %
    SHORT: TP is below avg_entry by take_profit_pct %
    """
    if direction.lower() == "long":
        return avg_entry * (1.0 + take_profit_pct / 100.0)
    else:
        return avg_entry * (1.0 - take_profit_pct / 100.0)


def tp_distance_pct(from_price: float, tp_price: float, direction: str) -> float:
    """Percentage distance from from_price to tp_price (reversal distance needed to reach TP)."""
    if from_price == 0:
        return 0.0
    if direction.lower() == "long":
        return (tp_price - from_price) / from_price * 100.0
    else:
        return (from_price - tp_price) / from_price * 100.0


# ─────────────────────────────────────────────────────────────
# Expected PnL estimate
# ─────────────────────────────────────────────────────────────

def expected_profit_usdt(
    filled_orders: list[tuple[float, float]],  # (price, qty)
    tp_price: float,
    direction: str,
) -> float:
    """Rough PnL estimate assuming all filled orders close at tp_price."""
    pnl = 0.0
    for entry_p, qty in filled_orders:
        if direction.lower() == "long":
            pnl += (tp_price - entry_p) * qty
        else:
            pnl += (entry_p - tp_price) * qty
    return pnl

