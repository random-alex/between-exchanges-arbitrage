import asyncio
import logging
from datetime import datetime
from .manager import PositionManager
from .models import Position
from app_ver2.instrument_fetcher import InstrumentFetcher
from app_ver2.utils import validate_close_liquidity_level1, estimate_slippage_simple

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
        # PHASE 1: Check if we should retry based on exponential backoff
        should_retry, wait_time = _should_retry_close(position)

        if not should_retry:
            # Too soon to retry - skip this check
            logger.debug(
                f"Skipping close attempt for position #{position.id} - "
                f"waiting {wait_time:.0f}s for retry backoff"
            )
            return
        # PHASE 1: Validate liquidity before attempting to close
        liquidity_check = validate_close_liquidity_level1(
            position, long_ticker, short_ticker, long_spec, short_spec
        )

        # Update close attempt tracking
        position.close_attempts += 1
        if position.first_close_attempt_at is None:
            position.first_close_attempt_at = datetime.now()
        position.last_close_attempt_at = datetime.now()

        # Estimate slippage for both legs
        required_qty = (
            position.remaining_quantity
            if position.remaining_quantity
            else position.quantity
        )
        long_slippage_pct = estimate_slippage_simple(
            required_qty, liquidity_check["available_long_qty"]
        )
        short_slippage_pct = estimate_slippage_simple(
            required_qty, liquidity_check["available_short_qty"]
        )
        total_slippage_estimate_usd = (
            long_slippage_pct / 100
        ) * required_qty * exit_long_price + (
            short_slippage_pct / 100
        ) * required_qty * exit_short_price
        position.actual_slippage_estimate = total_slippage_estimate_usd

        # Record close attempt
        await position_manager.record_close_attempt(
            position=position,
            long_bid_qnt=liquidity_check["available_long_qty"],
            short_ask_qnt=liquidity_check["available_short_qty"],
            attempted_long_price=exit_long_price,
            attempted_short_price=exit_short_price,
            attempted_spread_pct=current_spread_pct,
            success=False,  # Will update if successful
            failure_reason=None,
        )

        # Check if we should force close (stop loss or max time)
        force_close = (
            reason.startswith("stop_loss")
            or reason == "max_hold_time_exceeded"
            or reason == "max_hold_time_no_data"
        )

        if liquidity_check["can_close_full"]:
            # FULL CLOSE: Sufficient liquidity for complete position
            logger.info(
                f"✅ Sufficient liquidity for position #{position.id} | "
                f"{position.symbol} | Liquidity ratio: {liquidity_check['liquidity_ratio'] * 100:.1f}%"
            )

            await position_manager.close_position(
                position.id, position, current_spread, reason
            )

            # Record successful close attempt
            await position_manager.record_close_attempt(
                position=position,
                long_bid_qnt=liquidity_check["available_long_qty"],
                short_ask_qnt=liquidity_check["available_short_qty"],
                attempted_long_price=exit_long_price,
                attempted_short_price=exit_short_price,
                attempted_spread_pct=current_spread_pct,
                success=True,
                failure_reason=None,
                partial_close=False,
                closed_quantity=required_qty,
            )

        elif liquidity_check["can_close_partial"]:
            # PARTIAL CLOSE: Close what we can
            close_qty = liquidity_check["max_closeable_qty"]

            # Validate and adjust quantity to meet exchange requirements
            from app_ver2.utils import validate_and_adjust_quantity

            adjusted_qty = validate_and_adjust_quantity(
                close_qty, long_spec, short_spec
            )

            if adjusted_qty is None or adjusted_qty <= 0:
                logger.warning(
                    f"⚠️ Cannot adjust partial close qty {close_qty:.6f} to meet exchange requirements | "
                    f"Position #{position.id} | Min: {max(long_spec.min_order_qnt, short_spec.min_order_qnt):.6f}"
                )
                await _log_liquidity_warning(
                    position, liquidity_check, position_manager
                )
                await position_manager.record_close_attempt(
                    position=position,
                    long_bid_qnt=liquidity_check["available_long_qty"],
                    short_ask_qnt=liquidity_check["available_short_qty"],
                    attempted_long_price=exit_long_price,
                    attempted_short_price=exit_short_price,
                    attempted_spread_pct=current_spread_pct,
                    success=False,
                    failure_reason="quantity_below_exchange_minimum",
                )
                return

            close_qty = adjusted_qty

            logger.warning(
                f"⚠️ Partial liquidity for position #{position.id} | "
                f"{position.symbol} | "
                f"Closing {close_qty:.6f} of {required_qty:.6f} ({liquidity_check['liquidity_ratio'] * 100:.1f}%)"
            )

            await position_manager.close_position_partial(
                position, close_qty, current_spread, f"partial_{reason}"
            )

            # Record partial close attempt
            await position_manager.record_close_attempt(
                position=position,
                long_bid_qnt=liquidity_check["available_long_qty"],
                short_ask_qnt=liquidity_check["available_short_qty"],
                attempted_long_price=exit_long_price,
                attempted_short_price=exit_short_price,
                attempted_spread_pct=current_spread_pct,
                success=True,
                failure_reason=None,
                partial_close=True,
                closed_quantity=close_qty,
            )

        elif force_close:
            # FORCE CLOSE: Close anyway despite low liquidity (stop loss / max time)

            # Check for severe asymmetric liquidity during force close
            min_order_qty = max(long_spec.min_order_qnt, short_spec.min_order_qnt)
            long_has_any = liquidity_check["available_long_qty"] >= min_order_qty
            short_has_any = liquidity_check["available_short_qty"] >= min_order_qty

            asymmetric_msg = ""
            if long_has_any and not short_has_any:
                asymmetric_msg = " | ⚠️ ASYMMETRIC: Long has liquidity, Short does NOT"
            elif short_has_any and not long_has_any:
                asymmetric_msg = " | ⚠️ ASYMMETRIC: Short has liquidity, Long does NOT"
            elif not long_has_any and not short_has_any:
                asymmetric_msg = " | ⚠️ CRITICAL: BOTH sides have NO liquidity"

            logger.error(
                f"🚨 FORCE CLOSING position #{position.id} despite insufficient liquidity | "
                f"{position.symbol} | Reason: {reason} | "
                f"Liquidity: {liquidity_check['liquidity_ratio'] * 100:.1f}% | "
                f"Estimated slippage: ${total_slippage_estimate_usd:.2f}{asymmetric_msg}"
            )

            await position_manager.close_position(
                position.id, position, current_spread, f"forced_{reason}"
            )

            # Record forced close
            await position_manager.record_close_attempt(
                position=position,
                long_bid_qnt=liquidity_check["available_long_qty"],
                short_ask_qnt=liquidity_check["available_short_qty"],
                attempted_long_price=exit_long_price,
                attempted_short_price=exit_short_price,
                attempted_spread_pct=current_spread_pct,
                success=True,
                failure_reason="forced_close_insufficient_liquidity",
                partial_close=False,
                closed_quantity=required_qty,
            )

        else:
            # WAIT: Insufficient liquidity, not force close scenario

            # Check for asymmetric liquidity (one side can close, other cannot)
            min_order_qty = max(long_spec.min_order_qnt, short_spec.min_order_qnt)
            long_can_close = liquidity_check["available_long_qty"] >= required_qty
            short_can_close = liquidity_check["available_short_qty"] >= required_qty

            if long_can_close and not short_can_close:
                logger.warning(
                    f"⚠️ ASYMMETRIC LIQUIDITY: Position #{position.id} | "
                    f"{position.symbol} {position.long_exchange}/{position.short_exchange} | "
                    f"Long side CAN close ({liquidity_check['available_long_qty']:.6f} available) | "
                    f"Short side CANNOT ({liquidity_check['available_short_qty']:.6f} < {required_qty:.6f}) | "
                    f"In REAL trading this would create UNHEDGED EXPOSURE"
                )
            elif short_can_close and not long_can_close:
                logger.warning(
                    f"⚠️ ASYMMETRIC LIQUIDITY: Position #{position.id} | "
                    f"{position.symbol} {position.long_exchange}/{position.short_exchange} | "
                    f"Short side CAN close ({liquidity_check['available_short_qty']:.6f} available) | "
                    f"Long side CANNOT ({liquidity_check['available_long_qty']:.6f} < {required_qty:.6f}) | "
                    f"In REAL trading this would create UNHEDGED EXPOSURE"
                )

            await _log_liquidity_warning(position, liquidity_check, position_manager)

            # Record failed attempt
            await position_manager.record_close_attempt(
                position=position,
                long_bid_qnt=liquidity_check["available_long_qty"],
                short_ask_qnt=liquidity_check["available_short_qty"],
                attempted_long_price=exit_long_price,
                attempted_short_price=exit_short_price,
                attempted_spread_pct=current_spread_pct,
                success=False,
                failure_reason="insufficient_liquidity",
            )


def _should_retry_close(position: Position) -> tuple[bool, float]:
    """Check if enough time has passed to retry closing based on exponential backoff.

    Returns:
        (should_retry, seconds_until_next_retry)
    """
    from app_ver2.config import Config

    if position.close_attempts == 0:
        return True, 0

    # Calculate delay for this attempt using exponential backoff
    delay = min(
        Config.RETRY_INITIAL_DELAY_SEC
        * (Config.RETRY_BACKOFF_MULTIPLIER ** (position.close_attempts - 1)),
        Config.RETRY_MAX_DELAY_SEC,
    )

    # Check if enough time has passed since last attempt
    if position.last_close_attempt_at:
        time_since_last = (
            datetime.now() - position.last_close_attempt_at
        ).total_seconds()
        if time_since_last < delay:
            return False, delay - time_since_last

    return True, 0


async def _log_liquidity_warning(
    position: Position,
    liquidity_check: dict,
    position_manager: PositionManager,
):
    """Log liquidity warning and track warning count."""
    from app_ver2.config import Config

    position.close_liquidity_warnings += 1
    position.last_liquidity_warning_at = datetime.now()

    await position_manager.db.update_position(position)

    # Calculate next retry time
    _, seconds_until_retry = _should_retry_close(position)

    logger.warning(
        f"⚠️ Insufficient liquidity to close position #{position.id} | "
        f"{position.symbol} {position.long_exchange}/{position.short_exchange} | "
        f"Required: {position.remaining_quantity or position.quantity:.6f} | "
        f"Available long: {liquidity_check['available_long_qty']:.6f} | "
        f"Available short: {liquidity_check['available_short_qty']:.6f} | "
        f"Liquidity ratio: {liquidity_check['liquidity_ratio'] * 100:.1f}% | "
        f"Warning #{position.close_liquidity_warnings} | "
        f"Close attempt #{position.close_attempts} | "
        f"Next retry in {seconds_until_retry:.0f}s"
    )

    # Alert if warnings are excessive
    if position.close_liquidity_warnings >= Config.MAX_LIQUIDITY_WARNINGS:
        logger.error(
            f"🚨 STUCK POSITION ALERT: Position #{position.id} has {position.close_liquidity_warnings} "
            f"liquidity warnings! | {position.symbol} | "
            f"Position has been open for {(datetime.now() - position.created_at).total_seconds() / 3600:.1f} hours | "
            f"{position.close_attempts} close attempts"
        )

    # Force close if max attempts exceeded
    if position.close_attempts >= Config.MAX_CLOSE_ATTEMPTS:
        logger.error(
            f"🚨 MAX RETRIES EXCEEDED: Position #{position.id} has exceeded {Config.MAX_CLOSE_ATTEMPTS} "
            f"close attempts. This position will be force closed on next convergence check."
        )
