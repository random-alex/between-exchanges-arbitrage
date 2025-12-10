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
    buy_data = exchange_data.get(position.buy_exchange, {})
    sell_data = exchange_data.get(position.sell_exchange, {})

    buy_ticker = buy_data.get(position.symbol)
    sell_ticker = sell_data.get(position.symbol)

    if not buy_ticker or not sell_ticker:
        should_close, reason = await position_manager.should_close(position, None)
        if should_close:
            await position_manager.close_position(position.id, position, None, reason)
        return

    # Get current prices for exit
    # To close: sell where we bought (at bid), buy where we sold (at ask)
    exit_price_on_buy_exchange = (
        buy_ticker.bid_price
    )  # Sell at bid on exchange where we bought
    exit_price_on_sell_exchange = (
        sell_ticker.ask_price
    )  # Buy at ask on exchange where we sold

    # Current spread from position's perspective
    # Spread converged if this approaches zero or goes negative
    current_spread_pct = (
        (exit_price_on_buy_exchange - exit_price_on_sell_exchange)
        / exit_price_on_sell_exchange
    ) * 100

    # Get fee specs for exit fee calculation
    buy_spec = instrument_fetcher.get_spec(
        position.buy_exchange, position.buy_instrument
    )
    sell_spec = instrument_fetcher.get_spec(
        position.sell_exchange, position.sell_instrument
    )
    fee_pct = buy_spec.fee_pct + sell_spec.fee_pct

    # Build exit data for P&L calculation
    # Use clear naming: price on the exchange where we originally bought/sold
    current_spread = {
        "exit_price_on_buy_exchange": exit_price_on_buy_exchange,
        "exit_price_on_sell_exchange": exit_price_on_sell_exchange,
        "spread_pct": current_spread_pct,
        "notional_usd": position.quantity
        * (exit_price_on_buy_exchange + exit_price_on_sell_exchange)
        / 2,
        "total_fees_usd": position.quantity
        * (exit_price_on_buy_exchange + exit_price_on_sell_exchange)
        / 2
        * fee_pct
        / 100,
    }

    should_close, reason = await position_manager.should_close(position, current_spread)

    if should_close:
        await position_manager.close_position(
            position.id, position, current_spread, reason
        )
