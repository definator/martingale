"""
db.py – SQLite persistence layer (aiosqlite).

Tables:
  trades  – one row per open cycle per symbol
  orders  – limit/take-profit orders belonging to a trade
  events  – event log
"""

import aiosqlite
from loguru import logger

DB_PATH = "bot_state.db"


# ─────────────────────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────────────────────
CREATE_TRADES = """
CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT    NOT NULL,
    direction       TEXT    NOT NULL,
    try_number      INTEGER NOT NULL DEFAULT 1,
    status          TEXT    NOT NULL DEFAULT 'open',
    entry_price     REAL,
    entry_qty       REAL,
    tp_order_id     TEXT,
    tp_price        REAL,
    start_deposit   REAL,
    realized_pnl    REAL,
    created_at      TEXT    DEFAULT (datetime('now')),
    closed_at       TEXT
)
"""

CREATE_ORDERS = """
CREATE TABLE IF NOT EXISTS orders (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id         INTEGER NOT NULL REFERENCES trades(id),
    binance_order_id TEXT,
    symbol           TEXT    NOT NULL,
    side             TEXT    NOT NULL,
    order_type       TEXT    NOT NULL,
    price            REAL,
    qty              REAL,
    status           TEXT    NOT NULL DEFAULT 'NEW',
    order_index      INTEGER,
    created_at       TEXT    DEFAULT (datetime('now')),
    filled_at        TEXT
)
"""

CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id    INTEGER,
    symbol      TEXT,
    event_type  TEXT,
    payload     TEXT,
    ts          TEXT DEFAULT (datetime('now'))
)
"""


# ─────────────────────────────────────────────────────────────
# Init
# ─────────────────────────────────────────────────────────────
async def init_db():
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        await conn.execute(CREATE_TRADES)
        await conn.execute(CREATE_ORDERS)
        await conn.execute(CREATE_EVENTS)
        await conn.commit()
    logger.info("Database initialised at {}", DB_PATH)


# ─────────────────────────────────────────────────────────────
# Trade CRUD
# ─────────────────────────────────────────────────────────────
async def create_trade(symbol: str, direction: str, try_number: int,
                       start_deposit: float) -> int:
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute(
            "INSERT INTO trades (symbol, direction, try_number, start_deposit) VALUES (?, ?, ?, ?)",
            (symbol, direction, try_number, start_deposit),
        )
        await conn.commit()
        return cur.lastrowid


async def update_trade(trade_id: int, **kwargs):
    if not kwargs:
        return
    sets = ", ".join(f"{k} = ?" for k in kwargs)
    vals = list(kwargs.values()) + [trade_id]
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(f"UPDATE trades SET {sets} WHERE id = ?", vals)
        await conn.commit()


async def get_open_trades(symbol: str | None = None) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        if symbol:
            cur = await conn.execute(
                "SELECT * FROM trades WHERE status='open' AND symbol=?", (symbol,)
            )
        else:
            cur = await conn.execute("SELECT * FROM trades WHERE status='open'")
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def close_trade(trade_id: int, realized_pnl: float):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "UPDATE trades SET status='closed', realized_pnl=?, closed_at=datetime('now') WHERE id=?",
            (realized_pnl, trade_id),
        )
        await conn.commit()


# ─────────────────────────────────────────────────────────────
# Order CRUD
# ─────────────────────────────────────────────────────────────
async def create_order_record(trade_id: int, binance_order_id: str, symbol: str,
                               side: str, order_type: str, price: float | None,
                               qty: float, order_index: int) -> int:
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute(
            """INSERT INTO orders
               (trade_id, binance_order_id, symbol, side, order_type, price, qty, order_index)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (trade_id, str(binance_order_id), symbol, side, order_type, price, qty, order_index),
        )
        await conn.commit()
        return cur.lastrowid


async def update_order_status(binance_order_id: str, status: str, filled_at: str | None = None):
    async with aiosqlite.connect(DB_PATH) as conn:
        if filled_at:
            await conn.execute(
                "UPDATE orders SET status=?, filled_at=? WHERE binance_order_id=?",
                (status, filled_at, str(binance_order_id)),
            )
        else:
            await conn.execute(
                "UPDATE orders SET status=? WHERE binance_order_id=?",
                (status, str(binance_order_id)),
            )
        await conn.commit()


async def get_orders_for_trade(trade_id: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT * FROM orders WHERE trade_id=? ORDER BY order_index",
            (trade_id,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def get_open_orders_for_trade(trade_id: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT * FROM orders WHERE trade_id=? AND status='NEW' ORDER BY order_index",
            (trade_id,),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────
# Event log
# ─────────────────────────────────────────────────────────────
async def log_event(trade_id: int | None, symbol: str, event_type: str, payload: str):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT INTO events (trade_id, symbol, event_type, payload) VALUES (?,?,?,?)",
            (trade_id, symbol, event_type, payload),
        )
        await conn.commit()
