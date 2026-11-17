#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Offline grid / averaging / TP / *indicative* liquidation calculator.

Usage:
    python orders_calculator.py --config config.json --symbol JCTUSDT
    python orders_calculator.py --config config.json --symbol JCTUSDT --mode custom
    python orders_calculator.py --config config.json --symbol JCTUSDT --csv result.csv

No exchange connection, no credentials and NO order placement.

Assumptions and limitations:
  - start_deposit and subsequent sizes are order NOTIONALS in USDT, not margin.
  - custom 'priceDiffPct' is a multiplication FACTOR, despite its name.
  - legacy grid formula is assumed (strategy.py was not provided): total-linear
    price distance across orders_qty levels and geometric notional sizes.
  - mark price, execution slippage, commissions, funding, impact of other
    positions and changing risk brackets are not simulated.
  - liquidation is an approximate estimate, NOT the Binance liquidation price.
  - cross calculation requires actual wallet balance; isolated requires leverage.
  - user must supply symbol-specific maintenance margin parameters; absent these
    parameters liquidation is shown as unavailable, never guessed.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_HALF_UP, localcontext
from pathlib import Path
from typing import Any

HUNDRED = Decimal("100")
ONE = Decimal("1")
ZERO = Decimal("0")


class InputError(ValueError):
    """Configuration parameter error."""


def dec(raw: Any, name: str) -> Decimal:
    if isinstance(raw, bool) or raw is None:
        raise InputError(f"{name}: требуется число, получено {raw!r}")
    try:
        value = Decimal(str(raw))
    except (ValueError, InvalidOperation, TypeError):
        raise InputError(f"{name}: некорректное число {raw!r}") from None
    if not value.is_finite():
        raise InputError(f"{name}: число должно быть конечным")
    return value


def arg_or_cfg(arg: Any, cfg: dict, *keys: str) -> Any:
    if arg is not None:
        return arg
    for key in keys:
        if key in cfg:
            return cfg[key]
    return None


def require_positive(value: Decimal, name: str, *, allow_zero: bool = False) -> None:
    if value < ZERO or (value == ZERO and not allow_zero):
        raise InputError(f"{name}: должно быть {'>= 0' if allow_zero else '> 0'}")


def fmt(x: Decimal | None, digits: int = 8) -> str:
    if x is None:
        return "—"
    text = f"{x:.{digits}f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


def pct_format(x: Decimal | None) -> str:
    return "—" if x is None else f"{x:+.2f}%"


def nearest_tick(value: Decimal, tick: Decimal | None) -> Decimal:
    if tick is None:
        return value
    return (value / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick


def qty_for_notional(
    notional: Decimal, price: Decimal, step: Decimal | None,
    min_qty: Decimal | None, min_notional: Decimal | None
) -> Decimal:
    target = notional / price
    if min_qty is not None:
        target = max(target, min_qty)
    if min_notional is not None:
        target = max(target, min_notional / price)
    if step is not None:
        # Conservative quantity rounding UP for this standalone simulation.
        target = (target / step).to_integral_value(rounding=ROUND_CEILING) * step
    return target


@dataclass(frozen=True)
class Level:
    number: int
    kind: str
    distance_pct: Decimal
    multiplier: Decimal
    price: Decimal
    planned_usdt: Decimal
    qty: Decimal
    executed_usdt: Decimal
    cumulative_qty: Decimal
    cumulative_usdt: Decimal
    avg_entry: Decimal
    tp_price: Decimal
    reversal_pct: Decimal
    liq_price: Decimal | None
    liq_distance_pct: Decimal | None
    liq_warning: str

    def csv_dict(self, symbol: str) -> dict[str, str | int]:
        data = {"symbol": symbol, "level": self.number, "kind": self.kind}
        for key, value in asdict(self).items():
            if key in ("number", "kind"):
                continue
            data[key] = "" if value is None else str(value)
        return data


@dataclass(frozen=True)
class MarginModel:
    mode: str
    leverage: Decimal | None
    wallet_balance: Decimal | None
    mmr: Decimal | None
    maint_amount: Decimal
    other_pnl: Decimal
    other_maintenance: Decimal

    @classmethod
    def from_settings(cls, args: argparse.Namespace, cfg: dict) -> "MarginModel":
        mode = str(arg_or_cfg(args.margin_mode, cfg, "marginMode", "margin_mode") or "cross").lower()
        if mode not in ("cross", "isolated"):
            raise InputError("marginMode: допустимы cross или isolated")

        leverage_raw = arg_or_cfg(args.leverage, cfg, "leverage")
        lev = dec(leverage_raw, "leverage") if leverage_raw is not None else None
        if lev is not None and lev < ONE:
            raise InputError("leverage должно быть >= 1")

        wallet_raw = arg_or_cfg(args.wallet_balance, cfg, "walletBalanceUsdt", "wallet_balance_usdt")
        wallet = dec(wallet_raw, "walletBalanceUsdt") if wallet_raw is not None else None
        if wallet is not None:
            require_positive(wallet, "walletBalanceUsdt", allow_zero=True)

        mmr_raw = arg_or_cfg(args.mmr, cfg, "maintenanceMarginRate", "maintenance_margin_rate")
        mmr = dec(mmr_raw, "maintenanceMarginRate") if mmr_raw is not None else None
        if mmr is not None and not (ZERO <= mmr < ONE):
            raise InputError("maintenanceMarginRate: ожидается доля, например 0.005 (0.5%)")

        amount_raw = arg_or_cfg(args.maintenance_amount, cfg, "maintenanceAmount", "maintenance_amount")
        amount = dec(amount_raw, "maintenanceAmount") if amount_raw is not None else ZERO
        require_positive(amount, "maintenanceAmount", allow_zero=True)

        other_pnl_raw = arg_or_cfg(args.other_pnl, cfg, "otherPositionsUnrealizedPnl")
        other_pnl = dec(other_pnl_raw, "otherPositionsUnrealizedPnl") if other_pnl_raw is not None else ZERO
        other_mm_raw = arg_or_cfg(args.other_mm, cfg, "otherPositionsMaintenanceMargin")
        other_mm = dec(other_mm_raw, "otherPositionsMaintenanceMargin") if other_mm_raw is not None else ZERO
        require_positive(other_mm, "otherPositionsMaintenanceMargin", allow_zero=True)

        return cls(mode, lev, wallet, mmr, amount, other_pnl, other_mm)

    def liquidation(self, direction: str, qty: Decimal, quote: Decimal) -> Decimal | None:
        """Single-symbol/flat-bracket indicative mark-price liquidation threshold.

        Formula: (wallet-like collateral + other PnL + position PnL)
                 = (position notional at mark * MMR - maint_amount) + other_MM
        Other-position values are treated as CONSTANT inputs, not dynamic functions of mark.
        """
        if self.mmr is None:
            return None
        if self.mode == "cross":
            if self.wallet_balance is None:
                return None
            collateral = self.wallet_balance
            other_pnl, other_mm = self.other_pnl, self.other_maintenance
        else:
            if self.leverage is None:
                return None
            collateral = quote / self.leverage
            other_pnl = other_mm = ZERO

        if direction == "long":
            # collateral + other_pnl + qty * (mark - avg)
            #   = qty * mark * mmr - maint_amount + other_mm
            candidate = (quote - collateral - other_pnl - self.maint_amount + other_mm) / (qty * (ONE - self.mmr))
        else:
            # collateral + other_pnl + qty * (avg - mark)
            #   = qty * mark * mmr - maint_amount + other_mm
            candidate = (quote + collateral + other_pnl + self.maint_amount - other_mm) / (qty * (ONE + self.mmr))
        return candidate if candidate > ZERO else None


def _get_order_entry(item: Any, i: int) -> tuple[Decimal, Decimal]:
    # Accept both [{"distancePct": ...}] and [{"order1": {"distancePct": ...}}].
    if not isinstance(item, dict):
        raise InputError(f"orders[{i}]: ожидается JSON-объект")
    if len(item) == 1 and "distancePct" not in item:
        inner = next(iter(item.values()))
        if isinstance(inner, dict):
            item = inner
    if "distancePct" not in item or "priceDiffPct" not in item:
        raise InputError(f"orders[{i}]: нужны distancePct и priceDiffPct")
    d = dec(item["distancePct"], f"orders[{i}].distancePct")
    factor = dec(item["priceDiffPct"], f"orders[{i}].priceDiffPct")
    require_positive(d, f"orders[{i}].distancePct", allow_zero=True)
    require_positive(factor, f"orders[{i}].priceDiffPct")
    return d, factor


def calculate(cfg: dict, args: argparse.Namespace) -> tuple[list[Level], list[str], MarginModel, str]:
    symbol = str(cfg.get("symbol", "UNKNOWN"))
    raw_price = arg_or_cfg(args.start_price, cfg, "start_price", "startPrice")
    if raw_price is None:
        raise InputError(f"{symbol}: требуется start_price или startPrice")
    start_price = dec(raw_price, "start_price")
    start_deposit = dec(cfg.get("start_deposit"), "start_deposit")
    take_profit_pct = dec(cfg.get("take_profit"), "take_profit")
    require_positive(start_price, "start_price")
    require_positive(start_deposit, "start_deposit")
    require_positive(take_profit_pct, "take_profit", allow_zero=True)
    if take_profit_pct >= HUNDRED:
        raise InputError("take_profit: ожидается значение меньше 100%")
    direction = str(cfg.get("direction", "")).lower()
    if direction not in ("short", "long"):
        raise InputError("direction: допустимы short или long")
    trailing = cfg.get("trailing_take_profit", False)
    custom_raw = cfg.get("specifyEveryOrder", False)
    if not isinstance(custom_raw, bool):
        raise InputError("specifyEveryOrder должен быть true или false (JSON boolean)")
    if not isinstance(trailing, bool):
        raise InputError("trailing_take_profit должен быть true или false")
    custom = custom_raw if args.mode is None else args.mode == "custom"
    mode = "custom" if custom else "legacy"

    tick_raw = arg_or_cfg(args.tick_size, cfg, "tickSize", "tick_size")
    step_raw = arg_or_cfg(args.step_size, cfg, "stepSize", "step_size")
    min_qty_raw = arg_or_cfg(args.min_qty, cfg, "minQty", "min_qty")
    min_notional_raw = arg_or_cfg(args.min_notional, cfg, "minNotional", "min_notional")
    tick = dec(tick_raw, "tickSize") if tick_raw is not None else None
    step = dec(step_raw, "stepSize") if step_raw is not None else None
    min_qty = dec(min_qty_raw, "minQty") if min_qty_raw is not None else None
    min_notional = dec(min_notional_raw, "minNotional") if min_notional_raw is not None else None
    for label, number in (("tickSize", tick), ("stepSize", step), ("minQty", min_qty), ("minNotional", min_notional)):
        if number is not None:
            require_positive(number, label)

    margin = MarginModel.from_settings(args, cfg)
    warn: list[str] = []
    if tick is None or step is None:
        warn.append("Нет tickSize/stepSize: цены и объёмы НЕ округляются полностью по биржевым фильтрам.")
    else:
        warn.append("Округление модели: цена к ближайшему tick, количество ВВЕРХ по step; проверь соответствие exchange.round_* в боте.")
    if margin.mmr is None:
        warn.append("Для оценки ликвидации нужно указать maintenanceMarginRate (например, 0.005 — лишь пример, НЕ ставка JCTUSDT).")
    if margin.mode == "cross":
        if margin.wallet_balance is None:
            warn.append("Cross: без walletBalanceUsdt оценить ликвидацию нельзя.")
        else:
            warn.append("Cross: расчёт условный, другие позиции/маржа/PNL/fees/funding/риск-ступени могут изменить реальный уровень.")
    elif margin.leverage is None:
        warn.append("Isolated: без leverage оценить ликвидацию нельзя.")
    if margin.mode == "isolated":
        warn.append("Isolated: моделируется залог = сумма номиналов входов / leverage; бот из исходника использует Cross.")

    levels_input: list[tuple[Decimal, Decimal]] = []
    if custom:
        items = cfg.get("orders")
        if not isinstance(items, list):
            raise InputError("specifyEveryOrder=true: orders должен быть массивом")
        levels_input = [_get_order_entry(item, i) for i, item in enumerate(items, 1)]
    else:
        count = cfg.get("orders_qty")
        if isinstance(count, bool):
            raise InputError("orders_qty должно быть целым числом")
        try:
            count = int(count)
        except (TypeError, ValueError):
            raise InputError("orders_qty должно быть целым числом") from None
        if str(cfg.get("orders_qty")) != str(count):
            raise InputError("orders_qty должно быть целым числом")
        if count < 1:
            raise InputError("orders_qty должно быть не меньше 1")
        total_dist = dec(cfg.get("distance_pct"), "distance_pct")
        mult = dec(cfg.get("multiplier"), "multiplier")
        require_positive(total_dist, "distance_pct", allow_zero=True)
        require_positive(mult, "multiplier")
        grid_mode = args.legacy_grid_mode or cfg.get("legacyGridMode", "total-linear")
        if grid_mode not in ("total-linear", "per-order-compound"):
            raise InputError("legacyGridMode: total-linear или per-order-compound")
        warn.append("Legacy-формула ПРЕДПОЛОЖЕНА (strategy.py не предоставлен), проверь соответствие реальному боту!")
        if grid_mode == "total-linear":
            # Distances between adjacent levels are computed relative to the INITIAL price.
            levels_input = [(total_dist / Decimal(count - 1), mult)] * (count - 1) if count > 1 else []
        else:
            levels_input = [(total_dist, mult)] * (count - 1)
    if len(levels_input) > 10000:
        raise InputError("Слишком много уровней (максимум 10000)")

    grid_mode = args.legacy_grid_mode or cfg.get("legacyGridMode", "total-linear")
    prev_price = nearest_tick(start_price, tick)
    if prev_price <= ZERO:
        raise InputError("Начальная цена после округления стала <= 0")
    prev_usdt = start_deposit
    filled_qty = ZERO
    filled_quote = ZERO
    rows: list[Level] = []
    prior_liq: Decimal | None = None
    prior_price: Decimal | None = None

    with localcontext() as ctx:
        ctx.prec = 46
        for index in range(len(levels_input) + 1):
            if index == 0:
                price = prev_price
                usdt = start_deposit
                distance = ZERO
                mult = ONE
            else:
                distance, mult = levels_input[index - 1]
                usdt = prev_usdt * mult
                if not custom and grid_mode == "total-linear":
                    cumulative_distance = distance * index / HUNDRED
                    raw = start_price * (ONE + cumulative_distance if direction == "short" else ONE - cumulative_distance)
                else:
                    raw = prev_price * (ONE + distance / HUNDRED if direction == "short" else ONE - distance / HUNDRED)
                if raw <= ZERO:
                    raise InputError(f"Уровень {index+1}: цена <= 0. Уменьши distancePct/distance_pct.")
                price = nearest_tick(raw, tick)
                if price <= ZERO:
                    raise InputError(f"Уровень {index+1}: цена <= 0 после округления по tickSize.")
                if price == prev_price:
                    warn.append(f"Уровень {index+1}: цена совпала с предыдущей после округления.")

            qty = qty_for_notional(usdt, price, step, min_qty, min_notional)
            executed = qty * price
            filled_qty += qty
            filled_quote += executed
            avg = filled_quote / filled_qty
            tp_base = avg if trailing else rows[0].avg_entry if rows else avg
            tp_factor = (ONE - take_profit_pct / HUNDRED) if direction == "short" else (ONE + take_profit_pct / HUNDRED)
            tp = nearest_tick(tp_base * tp_factor, tick)
            if tp <= ZERO:
                raise InputError("TP после округления стал <= 0")
            reversal = ((price - tp) / price * HUNDRED) if direction == "short" else ((tp - price) / price * HUNDRED)
            liq = margin.liquidation(direction, filled_qty, filled_quote)
            liq_dist = None if liq is None else (
                ((liq - price) / price * HUNDRED) if direction == "short" else ((price - liq) / price * HUNDRED)
            )
            liq_warning = ""
            if liq_dist is not None and liq_dist <= ZERO:
                liq_warning = "ЛИКВ. ДО/НА КАСАНИИ"
            elif index > 0 and prior_liq is not None and prior_price is not None:
                crossed = (price >= prior_liq) if direction == "short" else (price <= prior_liq)
                if crossed:
                    liq_warning = "ПРОШЛЫЙ LIQ ДО УРОВНЯ"
            rows.append(Level(
                number=index+1,
                kind="MARKET" if index == 0 else "LIMIT",
                distance_pct=distance,
                multiplier=mult,
                price=price,
                planned_usdt=usdt,
                qty=qty,
                executed_usdt=executed,
                cumulative_qty=filled_qty,
                cumulative_usdt=filled_quote,
                avg_entry=avg,
                tp_price=tp,
                reversal_pct=reversal,
                liq_price=liq,
                liq_distance_pct=liq_dist,
                liq_warning=liq_warning,
            ))
            prev_price, prev_usdt, prior_liq, prior_price = price, usdt, liq, price

    if margin.leverage is not None and margin.mode == "cross" and margin.wallet_balance is not None:
        max_collateral_needed = max((r.cumulative_usdt / margin.leverage for r in rows), default=ZERO)
        if max_collateral_needed > margin.wallet_balance:
            warn.append("Суммарная начальная маржа сетки выше заданного walletBalanceUsdt: исполнение всех уровней может быть невозможно.")
    warn.append("Таблица предполагает полное исполнение каждого уровня по указанной цене, без комиссий и проскальзывания.")
    warn.append("Ликвидация определяется Binance по MARK PRICE, а не обязательно по цене сделки; это НЕ цена биржи.")
    return rows, warn, margin, mode


def render(symbol: str, cfg: dict, rows: list[Level], warnings: list[str], margin: MarginModel, mode: str) -> None:
    direction = str(cfg["direction"]).upper()
    trailing = bool(cfg.get("trailing_take_profit", False))
    print(f"\n{'=' * 112}\n{symbol} | {direction} | {mode.upper()} | TP {'TRAILING' if trailing else 'FIXED'} | маржа: {margin.mode.upper()}")
    print("Здесь LIMIT #2 = orders[0] (первый доп. ордер); #1 — стартовый MARKET. Все значения в USDT/монетах.")
    print("-" * 112)
    header = ["Ур.", "Цена касания", "Объём USDT", "Общ. USDT", "Ср. вход", "TP", "Разворот", "Ликв. ≈", "До ликв. ≈"]
    table: list[list[str]] = []
    for r in rows:
        table.append([
            str(r.number), fmt(r.price, 10), fmt(r.executed_usdt, 2), fmt(r.cumulative_usdt, 2),
            fmt(r.avg_entry, 10), fmt(r.tp_price, 10), pct_format(r.reversal_pct),
            fmt(r.liq_price, 10), pct_format(r.liq_distance_pct),
        ])
    widths = [max(len(header[j]), *(len(line[j]) for line in table)) for j in range(len(header))]
    print(" | ".join(h.rjust(widths[i]) for i, h in enumerate(header)))
    print("-+-".join("-" * w for w in widths))
    for r, line in zip(rows, table):
        print(" | ".join(cell.rjust(widths[i]) for i, cell in enumerate(line)))
        if r.liq_warning:
            print(f"    !!! Уровень {r.number}: {r.liq_warning}; последующие строки могут быть недостижимы.")
    print("\nПримечания:")
    for note in dict.fromkeys(warnings):
        print(" - " + note)
    print(f" - Цена последнего уровня: {fmt(rows[-1].price, 10)}; общий номинал при всех исполнениях: {fmt(rows[-1].cumulative_usdt, 4)} USDT")
    print(" - Процент разворота = движение цены от уровня его исполнения до TP (положительное значение = в прибыльную сторону).")
    print(" - Процент до ликвидации = изменение цены от уровня исполнения в убыточную сторону; отрицательный = условный порог уже пройден.")


def run() -> int:
    p = argparse.ArgumentParser(description="Офлайн-калькулятор усреднения ордеров (не отправляет ордера)")
    p.add_argument("--config", default="config.json", help="JSON конфигурация бота")
    p.add_argument("--symbol", help="Выбрать символ; без аргумента считаются все symbols")
    p.add_argument("--start-price", help="Подменить start_price; с несколькими symbols необходим --symbol")
    p.add_argument("--mode", choices=("legacy", "custom"), help="Принудительно выбрать режим для анализа, не изменяя config.json")
    p.add_argument("--legacy-grid-mode", choices=("total-linear", "per-order-compound"), help="Предполагаемая формула старой сетки")
    p.add_argument("--margin-mode", choices=("cross", "isolated"))
    p.add_argument("--leverage", help="Плечо, например 20")
    p.add_argument("--wallet-balance", help="Баланс фьючерсного кошелька в USDT для Cross")
    p.add_argument("--mmr", help="Maintenance Margin RATE долей, например 0.005 (не процент 0.5)")
    p.add_argument("--maintenance-amount", help="Binance maintenanceAmount в USDT для выбранного bracket")
    p.add_argument("--other-pnl", help="Сумма нереализованного PnL других позиций (приближение)")
    p.add_argument("--other-mm", help="Сумма поддерживающей маржи других позиций (приближение)")
    p.add_argument("--tick-size", help="Биржевой шаг цены")
    p.add_argument("--step-size", help="Биржевой шаг количества")
    p.add_argument("--min-qty", help="Биржевое минимальное количество")
    p.add_argument("--min-notional", help="Биржевой минимальный номинал USDT")
    p.add_argument("--csv", help="Необязательный путь для CSV-отчёта")
    args = p.parse_args()
    try:
        cfg_path = Path(args.config)
        config = json.loads(cfg_path.read_text(encoding="utf-8-sig"), parse_float=Decimal)
        if not isinstance(config, dict):
            raise InputError("Корень config.json должен быть объектом")
        symbols = config.get("symbols")
        if symbols is None and "symbol" in config:
            symbols = [config]
        if not isinstance(symbols, list) or not symbols:
            raise InputError("Ожидается массив symbols или конфигурация одного символа")
        if args.symbol:
            symbols = [s for s in symbols if isinstance(s, dict) and str(s.get("symbol", "")).upper() == args.symbol.upper()]
            if not symbols:
                raise InputError(f"Символ {args.symbol} не найден в config.json")
        if args.start_price is not None and len(symbols) > 1:
            raise InputError("При --start-price укажи один --symbol")
        csv_rows: list[dict[str, Any]] = []
        for cfg in symbols:
            if not isinstance(cfg, dict):
                raise InputError("Элемент symbols должен быть JSON-объектом")
            rows, warnings, margin, mode = calculate(cfg, args)
            render(str(cfg.get("symbol", "UNKNOWN")), cfg, rows, warnings, margin, mode)
            csv_rows.extend(row.csv_dict(str(cfg.get("symbol", "UNKNOWN"))) for row in rows)
        if args.csv:
            path = Path(args.csv)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(csv_rows[0]))
                writer.writeheader()
                writer.writerows(csv_rows)
            print(f"\nCSV сохранён: {path}")
        return 0
    except (InputError, OSError, json.JSONDecodeError, ZeroDivisionError) as e:
        print(f"ОШИБКА: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(run())
