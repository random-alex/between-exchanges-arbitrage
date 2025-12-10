"""Main application with improved WebSocket connectors."""

import asyncio
import logging
import time

from config import load_config
from connectors import BybitConnector, OKXConnector, BinanceConnector, DeribitConnector
from app_ver2.connectors.models import Ticker
from app_ver2.utils import log_to_csv_async
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

# Position Management Config
MIN_ROI_TO_OPEN = 2.0
STOP_LOSS_PCT = -10.0
TARGET_CONVERGENCE_PCT = 0.1
MAX_HOLD_TIME_HOURS = 24
MIN_SPREAD_PCT = 1.5


async def process_connector_messages(
    connector, data_store: dict[str, Ticker], name: str
) -> None:
    """Process messages from a connector's queue."""
    while True:
        try:
            ticker: Ticker = await connector.queue.get()

            # Check data staleness
            now = time.time() * 1000
            age_ms = now - ticker.ts
            if age_ms > connector.config.staleness_threshold * 1000:
                connector.logger.stale_data(
                    ticker.normalized_instrument_id, age_ms / 1000
                )
                continue

            # Store in shared data
            data_store[ticker.normalized_instrument_id] = ticker  # pyright: ignore[reportArgumentType]

        except Exception as e:
            main_logger.parse_error(e, "")


async def spread_monitor(
    exchange_data: dict[str, dict[str, Ticker]],
    instrument_fetcher: InstrumentFetcher,
    position_manager: PositionManager,
) -> None:
    """Monitor spreads between all exchange pairs."""
    logger.info("🔍 Spread monitor started")

    while True:
        await asyncio.sleep(1)

        exchanges = list(exchange_data.keys())
        if len(exchanges) < 2:
            continue

        # Collect all spreads grouped by symbol
        spreads_by_symbol = {}

        for i, exchange1 in enumerate(exchanges):
            for exchange2 in exchanges[i + 1 :]:
                data1 = exchange_data[exchange1]
                data2 = exchange_data[exchange2]

                if not data1 or not data2:
                    continue

                common_symbols = set(data1.keys()) & set(data2.keys())

                for symbol in common_symbols:
                    ticker1 = data1[symbol]
                    ticker2 = data2[symbol]

                    spread = calculate_spread(
                        ticker1,
                        ticker2,
                        instrument_fetcher,
                        capital=100,
                        leverage=10,
                        slippage=None,
                        min_spread_threshold=0.05,
                    )

                    if spread:
                        await log_to_csv_async(
                            spread, symbol=f"{exchange1}_{exchange2}_{symbol}"
                        )

                        if spread["is_profitable"] and spread["roi_pct"] > 0.5:
                            if symbol not in spreads_by_symbol:
                                spreads_by_symbol[symbol] = []

                            spreads_by_symbol[symbol].append(
                                {
                                    "spread": spread,
                                    "ticker1": ticker1,
                                    "ticker2": ticker2,
                                    "exchange1": exchange1,
                                    "exchange2": exchange2,
                                }
                            )

        # Process each symbol: open best spreads while avoiding exchange conflicts
        for symbol, opportunities in spreads_by_symbol.items():
            # Sort by ROI descending (best first)
            opportunities.sort(key=lambda x: x["spread"]["roi_pct"], reverse=True)

            # Track which exchanges are already used for this symbol
            used_exchanges = set()

            for opp in opportunities:
                spread = opp["spread"]
                exchange1 = opp["exchange1"]
                exchange2 = opp["exchange2"]

                # Check if exchanges already used in another position for this symbol
                if exchange1 in used_exchanges or exchange2 in used_exchanges:
                    continue

                # Log with cooldown to prevent spam (logs once per minute per opportunity)
                main_logger.log_opportunity(
                    exchange1,
                    exchange2,
                    symbol,
                    f"🔥 {exchange1.upper()}/{exchange2.upper()} {symbol}: "
                    f"{spread['roi_pct']:.4f}% ROI | "
                    f"Spread: {spread['spread_pct']:.4f}% | "
                    f"Profit: ${spread['net_profit_usd']:.4f} | "
                    f"Buy: {spread['buy_exchange']} @ ${spread['buy_price']:.4f} | "
                    f"Sell: {spread['sell_exchange']} @ ${spread['sell_price']:.4f}",
                    cooldown=60.0,
                )

                can_open, reason = await position_manager.should_open(
                    spread, symbol, exchange1, exchange2
                )
                if can_open:
                    try:
                        await position_manager.open_position(
                            spread, opp["ticker1"], opp["ticker2"], symbol
                        )
                        # Mark exchanges as used
                        used_exchanges.add(exchange1)
                        used_exchanges.add(exchange2)
                    except Exception as e:
                        logger.error(f"Failed to open position: {e}")


async def stats_monitor(*connectors) -> None:
    """Monitor and log statistics periodically."""
    while True:
        await asyncio.sleep(300)  # Every 5 minutes

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
        min_roi=MIN_ROI_TO_OPEN,
        stop_loss_pct=STOP_LOSS_PCT,
        target_convergence_pct=TARGET_CONVERGENCE_PCT,
        max_hold_hours=MAX_HOLD_TIME_HOURS,
        min_spread_cpt=MIN_SPREAD_PCT,
    )

    # Create rate-limited loggers for each connector
    connector_loggers = {
        name: RateLimitedLogger(name, logger, window=10.0) for name in configs.keys()
    }

    # Initialize connectors with their loggers
    connectors = {
        "bybit": BybitConnector(configs["bybit"], connector_loggers["bybit"]),
        "okx": OKXConnector(configs["okx"], connector_loggers["okx"]),
        "binance": BinanceConnector(configs["binance"], connector_loggers["binance"]),
        "deribit": DeribitConnector(configs["deribit"], connector_loggers["deribit"]),
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
