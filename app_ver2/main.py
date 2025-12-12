"""Main application with improved WebSocket connectors."""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from config import load_config, Config
from connectors import (
    BybitConnector,
    OKXConnector,
    BinanceConnector,
    DeribitConnector,
    BitgetConnector,
)
from app_ver2.connectors.models import Ticker
from utils import calculate_spread
from rate_limited_logger import RateLimitedLogger
from instrument_fetcher import InstrumentFetcher
from app_ver2.position_manager.database import PositionDB
from app_ver2.position_manager.manager import PositionManager
from app_ver2.position_manager.monitor import position_monitor

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
main_logger = RateLimitedLogger(__name__, base_logger=logger)

# Suppress noisy third-party library logs
logging.getLogger("pybit").setLevel(logging.WARNING)
logging.getLogger("okx").setLevel(logging.WARNING)
logging.getLogger("websocket").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)


def _find_spreads(
    exchange_data: dict[str, dict[str, Ticker]], instrument_fetcher: InstrumentFetcher
) -> dict[str, list]:
    """Find all profitable spreads across exchanges."""
    spreads_by_symbol = {}
    exchanges = list(exchange_data.keys())

    for i, exchange1 in enumerate(exchanges):
        for exchange2 in exchanges[i + 1 :]:
            data1, data2 = exchange_data[exchange1], exchange_data[exchange2]
            if not data1 or not data2:
                continue

            for symbol in set(data1.keys()) & set(data2.keys()):
                spread = calculate_spread(
                    data1[symbol],
                    data2[symbol],
                    instrument_fetcher,
                    capital=Config.CAPITAL,
                    leverage=Config.LEVERAGE,
                    slippage=None,
                    min_spread_threshold=Config.MIN_SPREAD_THRESHOLD,
                )

                if (
                    spread
                    and spread["is_profitable"]
                    and spread["roi_pct"] > Config.MIN_ROI_FOR_LOGGING
                ):
                    if symbol not in spreads_by_symbol:
                        spreads_by_symbol[symbol] = []
                    spreads_by_symbol[symbol].append(
                        {
                            "spread": spread,
                            "ticker1": data1[symbol],
                            "ticker2": data2[symbol],
                            "exchange1": exchange1,
                            "exchange2": exchange2,
                        }
                    )

    # Sort by ROI
    for symbol in spreads_by_symbol:
        spreads_by_symbol[symbol].sort(
            key=lambda x: x["spread"]["roi_pct"], reverse=True
        )

    return spreads_by_symbol


async def spread_monitor(
    exchange_data: dict[str, dict[str, Ticker]],
    instrument_fetcher: InstrumentFetcher,
    position_manager: PositionManager,
    executor: ThreadPoolExecutor,
) -> None:
    """Monitor spreads between exchanges."""
    logger.info("🔍 Spread monitor started")

    while True:
        await asyncio.sleep(Config.SPREAD_CHECK_INTERVAL)

        exchanges = list(exchange_data.keys())
        if len(exchanges) < 2:
            continue

        # Offload CPU-heavy spread calculation to thread pool
        loop = asyncio.get_event_loop()
        spreads_by_symbol = await loop.run_in_executor(
            executor, _find_spreads, exchange_data, instrument_fetcher
        )

        for symbol, opportunities in spreads_by_symbol.items():
            used_exchanges = set()

            for opp in opportunities:
                spread, ex1, ex2 = opp["spread"], opp["exchange1"], opp["exchange2"]

                if ex1 in used_exchanges or ex2 in used_exchanges:
                    continue

                if spread["spread_pct"] > Config.MIN_SPREAD_PCT:
                    await main_logger.log_opportunity(
                        ex1,
                        ex2,
                        symbol,
                        f"🔥 {ex1.upper()}/{ex2.upper()} {symbol}: "
                        f"{spread['roi_pct']:.4f}% ROI | "
                        f"Spread: {spread['spread_pct']:.4f}% | "
                        f"Profit: ${spread['net_profit_usd']:.4f} | "
                        f"Long: {spread['long_exchange']} @ ${spread['entry_long_price']:.4f} | "
                        f"Short: {spread['short_exchange']} @ ${spread['entry_short_price']:.4f}",
                        cooldown=Config.LOG_COOLDOWN,
                    )

                can_open, _ = await position_manager.should_open(
                    spread, symbol, ex1, ex2
                )
                if can_open:
                    try:
                        await position_manager.open_position(
                            spread, opp["ticker1"], opp["ticker2"], symbol
                        )
                        used_exchanges.add(ex1)
                        used_exchanges.add(ex2)
                    except Exception as e:
                        logger.error(f"Failed to open position: {e}")


async def stats_monitor(*connectors) -> None:
    """Monitor and log statistics periodically."""
    while True:
        await asyncio.sleep(Config.STATS_INTERVAL)

        # Log connector stats
        stats_lines = ["📈 Connector Statistics:"]
        statuses = []
        for connector in connectors:
            stats = connector.logger.get_stats()
            stats_lines.append(
                f"  • {connector.config.name}: "
                f"{stats['parse_errors']} parse errors, "
                f"{stats['connection_errors']} conn errors"
            )
            status = "✅" if connector.is_connected() else "❌"
            statuses.append(f"{connector.config.name.capitalize()} {status}")

        logger.info("\n".join(stats_lines))
        logger.info(f"Health: {' | '.join(statuses)}")


async def event_loop_monitor() -> None:
    """Detect event loop blocking."""
    logger.info("⚡ Event loop monitor started")

    while True:
        start = time.perf_counter()
        await asyncio.sleep(0)
        lag_ms = (time.perf_counter() - start) * 1000

        if lag_ms > 50:
            logger.warning(f"⚠️  Event loop lag: {lag_ms:.0f}ms")

        await asyncio.sleep(5.0)


async def main():
    """Main application entry point."""
    # Load configurations
    configs = load_config()

    # Fetch instrument specifications
    logger.info("📡 Fetching instrument specifications...")
    instrument_fetcher = InstrumentFetcher()
    await instrument_fetcher.fetch_all(configs)

    # Initialize position system
    logger.info("💾 Initializing position database...")
    db = PositionDB("data/positions.db")
    await db.initialize()

    position_manager = PositionManager(
        db=db,
        min_roi=Config.MIN_ROI_TO_OPEN,
        stop_loss_pct=Config.STOP_LOSS_PCT,
        target_convergence_pct=Config.TARGET_CONVERGENCE_PCT,
        max_hold_hours=Config.MAX_HOLD_TIME_HOURS,
        min_spread_cpt=Config.MIN_SPREAD_PCT,
    )

    # Shared data stores - one per exchange (created first)
    exchange_data: dict[str, dict[str, Ticker]] = {name: {} for name in configs.keys()}

    # Create rate-limited loggers and connectors
    CONNECTOR_CLASSES = {
        "bybit": BybitConnector,
        "okx": OKXConnector,
        "binance": BinanceConnector,
        "deribit": DeribitConnector,
        "bitget": BitgetConnector,
    }

    connectors = {
        name: CONNECTOR_CLASSES[name](
            config,
            RateLimitedLogger(name, logger, window=Config.LOGGER_WINDOW),
            exchange_data[name],
        )
        for name, config in configs.items()
    }

    # Create thread pool for CPU-bound spread calculations
    executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="spread_calc")

    logger.info(
        f"🚀 Starting application with {len(connectors)} exchanges: {', '.join(connectors.keys())}..."
    )

    try:
        # Build task list dynamically
        tasks = []

        # Connection tasks (connectors write directly to exchange_data)
        for connector in connectors.values():
            tasks.append(connector.connect_with_retry())

        # Monitoring tasks
        tasks.append(
            spread_monitor(
                exchange_data, instrument_fetcher, position_manager, executor
            )
        )
        tasks.append(
            position_monitor(position_manager, exchange_data, instrument_fetcher)
        )
        tasks.append(stats_monitor(*connectors.values()))
        tasks.append(event_loop_monitor())

        # Run all components concurrently
        await asyncio.gather(*tasks)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
        for connector in connectors.values():
            await connector.stop()
        await instrument_fetcher.close()
        await db.close()
        executor.shutdown(wait=True)


if __name__ == "__main__":
    asyncio.run(main())
