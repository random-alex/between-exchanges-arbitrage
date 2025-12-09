"""Main application with improved WebSocket connectors."""

import asyncio
import logging
import sys
import time
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import load_config
from connectors import BybitConnector, OKXConnector, BinanceConnector, DeribitConnector
from app.connectors.models import Ticker
from app.connectors.run_all import log_to_csv
from utils import calculate_spread

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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
            ticker = await connector.queue.get()

            # Check data staleness
            now = time.time() * 1000
            if now - ticker.ts > connector.config.staleness_threshold * 1000:
                logger.warning(
                    f"[{name}] Stale data detected for {ticker.instrument_id}: "
                    f"{(now - ticker.ts) / 1000:.1f}s old"
                )
                continue

            # Store in shared data (note: using original typo'd attribute name)
            data_store[ticker.normilized_intrument_id] = ticker

        except Exception as e:
            logger.error(f"[{name}] Message processing error: {e}")


async def spread_monitor(exchange_data: dict[str, dict[str, Ticker]]) -> None:
    """Monitor spreads between all exchange pairs."""
    logger.info("🔍 Spread monitor started")

    while True:
        await asyncio.sleep(2)

        # Get all exchange names
        exchanges = list(exchange_data.keys())

        if len(exchanges) < 2:
            continue

        # Compare all exchange pairs
        for i, exchange1 in enumerate(exchanges):
            for exchange2 in exchanges[i + 1 :]:
                data1 = exchange_data[exchange1]
                data2 = exchange_data[exchange2]

                if not data1 or not data2:
                    continue

                # Find common instruments
                common_symbols = set(data1.keys()) & set(data2.keys())

                for symbol in common_symbols:
                    ticker1 = data1[symbol]
                    ticker2 = data2[symbol]

                    # Calculate spread with dynamic slippage
                    spread = calculate_spread(
                        ticker1,
                        ticker2,
                        capital=100,
                        leverage=10,
                        slippage=None,  # Dynamic based on liquidity
                        min_spread_threshold=-15,  # Skip spreads < 0.15%
                    )

                    if spread:
                        # Log all calculations
                        log_to_csv(spread, symbol=f"{exchange1}_{exchange2}_{symbol}")

                        # Print profitable opportunities
                        if spread["is_profitable"] and spread["roi_pct"] > 0.5:
                            logger.info(
                                f"🔥 {exchange1.upper()}/{exchange2.upper()} {symbol}: "
                                f"{spread['roi_pct']:.2f}% ROI | "
                                f"Spread: {spread['spread_pct']:.3f}% | "
                                f"Profit: ${spread['net_profit_usd']:.2f} | "
                                f"Buy: {spread['buy_exchange']} @ ${spread['buy_price']:.2f} | "
                                f"Sell: {spread['sell_exchange']} @ ${spread['sell_price']:.2f}"
                            )


async def health_monitor(*connectors) -> None:
    """Monitor connector health for all exchanges."""
    while True:
        await asyncio.sleep(60)

        statuses = []
        for connector in connectors:
            status = "✅" if connector.is_connected() else "❌"
            statuses.append(f"{connector.config.name.capitalize()} {status}")

        logger.info(f"Health: {' | '.join(statuses)}")


async def main():
    """Main application entry point."""
    # Load configurations
    configs = load_config()

    # Initialize connectors dynamically
    connectors = {
        "bybit": BybitConnector(configs["bybit"]),
        "okx": OKXConnector(configs["okx"]),
        "binance": BinanceConnector(configs["binance"]),
        "deribit": DeribitConnector(configs["deribit"]),
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
        tasks.append(spread_monitor(exchange_data))
        tasks.append(health_monitor(*connectors.values()))

        # Run all components concurrently
        await asyncio.gather(*tasks)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
        for connector in connectors.values():
            await connector.stop()


if __name__ == "__main__":
    asyncio.run(main())
