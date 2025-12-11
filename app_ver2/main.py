"""Main application with improved WebSocket connectors."""

import asyncio
import logging

from config import load_config, Config
from connectors import BybitConnector, OKXConnector, BinanceConnector, DeribitConnector
from app_ver2.connectors.models import Ticker
from utils import calculate_spread
from rate_limited_logger import RateLimitedLogger
from instrument_fetcher import InstrumentFetcher
from position_manager import PositionDB, PositionManager, position_monitor

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


async def process_connector_messages(
    connector, data_store: dict[str, Ticker], name: str
) -> None:
    """Process messages from a connector's queue."""
    while True:
        try:
            ticker: Ticker = await asyncio.wait_for(
                connector.queue.get(), timeout=Config.QUEUE_TIMEOUT
            )
            data_store[ticker.normalized_instrument_id] = ticker  # pyright: ignore[reportArgumentType]
        except asyncio.TimeoutError:
            logger.warning(f"{name}: No messages for {Config.QUEUE_TIMEOUT}s")
        except Exception as e:
            main_logger.parse_error(e, "")


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
) -> None:
    """Monitor spreads between exchanges."""
    logger.info("🔍 Spread monitor started")

    while True:
        await asyncio.sleep(Config.SPREAD_CHECK_INTERVAL)

        exchanges = list(exchange_data.keys())
        if len(exchanges) < 2:
            continue

        spreads_by_symbol = _find_spreads(exchange_data, instrument_fetcher)

        for symbol, opportunities in spreads_by_symbol.items():
            used_exchanges = set()

            for opp in opportunities:
                spread, ex1, ex2 = opp["spread"], opp["exchange1"], opp["exchange2"]

                if ex1 in used_exchanges or ex2 in used_exchanges:
                    continue

                main_logger.log_opportunity(
                    ex1,
                    ex2,
                    symbol,
                    f"🔥 {ex1.upper()}/{ex2.upper()} {symbol}: "
                    f"{spread['roi_pct']:.4f}% ROI | "
                    f"Spread: {spread['spread_pct']:.4f}% | "
                    f"Profit: ${spread['net_profit_usd']:.4f} | "
                    f"Buy: {spread['buy_exchange']} @ ${spread['buy_price']:.4f} | "
                    f"Sell: {spread['sell_exchange']} @ ${spread['sell_price']:.4f}",
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

        # Force summary of rate-limited logs
        for connector in connectors:
            connector.logger.force_summary()

        # Log connector stats
        stats_lines = ["📈 Connector Statistics (last 5 min):"]
        statuses = []
        for connector in connectors:
            stats = connector.logger.get_stats()
            stats_lines.append(
                f"  • {connector.config.name}: "
                f"{stats['parse_errors']} parse errors, "
                f"{stats['queue_drops']} drops, "
                f"{stats['connection_errors']} conn errors"
            )
            status = "✅" if connector.is_connected() else "❌"
            statuses.append(f"{connector.config.name.capitalize()} {status}")

        logger.info("\n".join(stats_lines))
        logger.info(f"Health: {' | '.join(statuses)}")


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

    # Create rate-limited loggers and connectors
    CONNECTOR_CLASSES = {
        "bybit": BybitConnector,
        "okx": OKXConnector,
        "binance": BinanceConnector,
        "deribit": DeribitConnector,
    }

    connectors = {
        name: CONNECTOR_CLASSES[name](
            config, RateLimitedLogger(name, logger, window=Config.LOGGER_WINDOW)
        )
        for name, config in configs.items()
    }

    # Shared data stores - one per exchange
    exchange_data: dict[str, dict[str, Ticker]] = {
        name: {} for name in connectors.keys()
    }

    logger.info(
        f"🚀 Starting application with {len(connectors)} exchanges: {', '.join(connectors.keys())}..."
    )

    try:
        # Build task list dynamically
        tasks = []

        # Connection tasks
        for connector in connectors.values():
            tasks.append(connector.connect_with_retry())

        # Message processor tasks
        for name, connector in connectors.items():
            tasks.append(
                process_connector_messages(connector, exchange_data[name], name)
            )

        # Monitoring tasks
        tasks.append(
            spread_monitor(exchange_data, instrument_fetcher, position_manager)
        )
        tasks.append(
            position_monitor(position_manager, exchange_data, instrument_fetcher)
        )
        tasks.append(stats_monitor(*connectors.values()))

        # Run all components concurrently
        await asyncio.gather(*tasks)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
        for connector in connectors.values():
            await connector.stop()
        await instrument_fetcher.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
