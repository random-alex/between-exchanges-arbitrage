import asyncio
import logging
from .manager import PositionManager
from .models import Position
from app_ver2.instrument_fetcher import InstrumentFetcher

logger = logging.getLogger(__name__)


async def position_monitor(
    position_manager: PositionManager,
    exchange_data: dict,
    instrument_fetcher: InstrumentFetcher,
):
    logger.info("🔄 Position monitor started")

    while True:
        await asyncio.sleep(5)

        try:
            open_positions = await position_manager.db.get_open_positions()

            if not open_positions:
                continue

            logger.debug(f"Monitoring {len(open_positions)} open positions")

            for position in open_positions:
                try:
                    await _check_position(
                        position, position_manager, exchange_data, instrument_fetcher
                    )
                except Exception as e:
                    logger.error(
                        f"Error checking position #{position.id}: {e}", exc_info=True
                    )

        except Exception as e:
            logger.error(f"Error in position monitor: {e}", exc_info=True)


async def _check_position(
    position: Position,
    position_manager: PositionManager,
    exchange_data: dict,
    instrument_fetcher: InstrumentFetcher,
):
    long_data = exchange_data.get(position.long_exchange, {})
    short_data = exchange_data.get(position.short_exchange, {})

    long_ticker = long_data.get(position.symbol)
    short_ticker = short_data.get(position.symbol)

    if not long_ticker or not short_ticker:
        return

    # Get current prices for exit
    # To close long: sell at bid on long exchange
    # To close short: buy at ask on short exchange
    exit_long_price = long_ticker.bid_price
    exit_short_price = short_ticker.ask_price

    # Current spread from position's perspective
    # Spread converged if this approaches zero or goes negative
    current_spread_pct = ((exit_long_price - exit_short_price) / exit_short_price) * 100

    # Get fee specs for exit fee calculation (separate for each exchange)
    long_spec = instrument_fetcher.get_spec(
        position.long_exchange, position.buy_instrument
    )
    short_spec = instrument_fetcher.get_spec(
        position.short_exchange, position.sell_instrument
    )

    # Build exit data for P&L calculation
    current_spread = {
        "exit_long_price": exit_long_price,
        "exit_short_price": exit_short_price,
        "spread_pct": current_spread_pct,
        "long_fee_pct": long_spec.fee_pct,
        "short_fee_pct": short_spec.fee_pct,
    }

    should_close, reason = await position_manager.should_close(position, current_spread)

    if should_close and reason:
        await position_manager.close_position(
            position.id, position, current_spread, reason
        )
