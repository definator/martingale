"""
main.py – Entry point for the Binance UM Futures martingale grid bot.

Usage:
  python main.py [--config config.json]

Workflow:
  1. Load config.json
  2. Init DB
  3. Create Binance AsyncClient + BinanceSocketManager
  4. Start Notifier background worker
  5. For each symbol in config, spawn a SymbolTrader coroutine
  6. All traders run concurrently via asyncio
  7. Graceful shutdown on SIGINT / SIGTERM (log metrics, drain notifier)
"""

import asyncio
import json
import signal
import sys
import argparse
from pathlib import Path

from binance import AsyncClient, BinanceSocketManager
from loguru import logger

import db
from notifier import Notifier
from symbol_trader import SymbolTrader


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

def setup_logging():
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO",
        colorize=True,
    )
    logger.add(
        "logs/bot_{time:YYYY-MM-DD}.log",
        rotation="00:00",
        retention="30 days",
        level="DEBUG",
        encoding="utf-8",
    )


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

async def main(config_path: str):
    setup_logging()
    Path("logs").mkdir(exist_ok=True)

    # Load config
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    logger.info("Config loaded from {}", config_path)

    # Init DB
    await db.init_db()

    # Create Binance client
    testnet = cfg.get("use_testnet", False)
    client  = await AsyncClient.create(
        api_key=cfg["api_key"],
        api_secret=cfg["api_secret"],
        testnet=testnet,
    )
    bsm = BinanceSocketManager(client)
    logger.info("Binance client ready  testnet={}", testnet)

    # Notifier (background queue worker started immediately)
    notifier = Notifier(
        enabled=cfg.get("telegram_enabled", False),
        bot_token=cfg.get("telegram_bot_token", ""),
        chat_id=cfg.get("telegram_chat_id", ""),
    )
    notifier.start_worker()

    # Build traders
    traders: list[SymbolTrader] = []
    for sym_cfg in cfg.get("symbols", []):
        trader = SymbolTrader(
            client=client,
            bsm=bsm,
            notifier=notifier,
            symbol_cfg=sym_cfg,
        )
        await trader.initialize()
        traders.append(trader)
        logger.info("Trader initialized for {}", sym_cfg["symbol"])

    # Graceful shutdown handler
    loop  = asyncio.get_running_loop()
    tasks: list[asyncio.Task] = []

    def _handle_signal():
        logger.info("Shutdown signal received.")
        for t in traders:
            t.stop()
        for task in tasks:
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            # Windows
            pass

    # Launch all traders concurrently
    async def run_trader(trader: SymbolTrader):
        try:
            await trader.run()
        except asyncio.CancelledError:
            logger.info("{}: task cancelled.", trader.symbol)
        except Exception as e:
            logger.exception("{}: unhandled exception: {}", trader.symbol, e)
            await notifier.error(trader.symbol, f"Критическая ошибка: {e}")
        finally:
            trader.log_metrics()

    tasks = [asyncio.create_task(run_trader(t)) for t in traders]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        # Drain notifier queue before exit
        await notifier.stop_worker()
        await client.close_connection()
        logger.info("Bot stopped. Binance connection closed.")


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Binance UM Futures Martingale Grid Bot")
    parser.add_argument(
        "--config", default="config.json",
        help="Path to config JSON file (default: config.json)"
    )
    args = parser.parse_args()

    try:
        asyncio.run(main(args.config))
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
