import logging
from datetime import datetime
from typing import Optional

from .models import Position, CloseAttempt
from .database import PositionDB

logger = logging.getLogger(__name__)


class PositionManager:
    def __init__(
        self,
        db: PositionDB,
        min_roi: float,
        stop_loss_pct: float,
        target_convergence_pct: float,
        max_hold_hours: int,
        min_spread_cpt: float,
    ):
        self.db = db
        self.min_roi = min_roi
        self.min_spread_cpt = min_spread_cpt
        self.stop_loss_pct = stop_loss_pct
        self.target_convergence_pct = target_convergence_pct
        self.max_hold_hours = max_hold_hours

    async def should_open(
        self, spread: dict, symbol: str, exchange1: str, exchange2: str
    ) -> tuple[bool, str]:
        if spread["roi_pct"] < self.min_roi:
            return False, f"roi_too_low_{spread['roi_pct']:.2f}"

        if spread["spread_pct"] < self.min_spread_cpt:
            return False, f"spread_too_low_{spread['spread_pct']:.2f}"

        # Check if these specific exchanges already have an open position for this symbol
        if await self.db.has_open_position_for_symbol_and_exchanges(
            symbol, exchange1, exchange2
        ):
            return False, "exchanges_already_used_for_symbol"

        if (
            spread.get("quantity_adjusted")
            and spread["btc_amount"] < spread["buy_min_qty"]
        ):
            return False, "quantity_too_small_after_adjustment"

        return True, "criteria_met"

    async def open_position(self, spread: dict, ticker1, ticker2, symbol: str) -> int:
        buy_instrument = (
            ticker1.instrument_id
            if spread["long_exchange"] == ticker1.exchange
            else ticker2.instrument_id
        )
        sell_instrument = (
            ticker1.instrument_id
            if spread["short_exchange"] == ticker1.exchange
            else ticker2.instrument_id
        )

        position = Position(
            symbol=symbol,
            long_exchange=spread["long_exchange"],
            buy_instrument=buy_instrument,
            short_exchange=spread["short_exchange"],
            sell_instrument=sell_instrument,
            entry_long_price=spread["entry_long_price"],
            entry_short_price=spread["entry_short_price"],
            entry_spread_pct=spread["spread_pct"],
            quantity=spread["btc_amount"],
            notional_usd=spread["notional_usd"],
            margin_used_usd=spread["margin_used_usd"],
            entry_fees_usd=spread["entry_fees_usd"],
            entry_buy_min_qty=spread["buy_min_qty"],
            entry_sell_min_qty=spread["sell_min_qty"],
            entry_buy_qty_step=spread["buy_qty_step"],
            entry_sell_qty_step=spread["sell_qty_step"],
            leverage=spread["leverage"],
            capital_allocated=spread["margin_used_usd"],
            open_reason=f"roi_{spread['roi_pct']:.2f}_spread_{spread['spread_pct']:.4f}",
        )

        position_id = await self.db.create_position(position)

        logger.info(
            f"📈 Opened position #{position_id} | "
            f"{symbol} | {spread['long_exchange']}/{spread['short_exchange']} | "
            f"Entry spread: {spread['spread_pct']:.4f}% | "
            f"Quantity: {spread['btc_amount']:.6f} | "
            f"Expected ROI: {spread['roi_pct']:.2f}%"
        )

        return position_id

    async def should_close(
        self, position: Position, current_spread: Optional[dict]
    ) -> tuple[bool, Optional[str]]:
        hold_hours = (datetime.now() - position.created_at).total_seconds() / 3600

        if current_spread is None:
            if hold_hours >= self.max_hold_hours:
                return True, "max_hold_time_no_data"
            return False, None

        if current_spread["spread_pct"] >= self.target_convergence_pct:
            return True, "convergence_target_reached"

        spread_change = current_spread["spread_pct"] - position.entry_spread_pct
        if spread_change <= self.stop_loss_pct:
            return True, f"stop_loss_widened_{spread_change:.2f}pct"

        if hold_hours >= self.max_hold_hours:
            return True, "max_hold_time_exceeded"

        return False, None

    async def close_position(
        self,
        position_id: int,
        position: Position,
        exit_spread: dict,
        close_reason: str,
    ):
        pnl = self._calculate_pnl(position, exit_spread)
        exit_data = {
            "exit_long_price": exit_spread["exit_long_price"],
            "exit_short_price": exit_spread["exit_short_price"],
            "exit_spread_pct": pnl["exit_spread_pct"],
            "gross_profit_usd": pnl["gross_pnl_usd"],
            "exit_fees_usd": pnl["exit_fees_usd"],
            "total_fees_usd": pnl["total_fees_usd"],
            "net_profit_usd": pnl["net_pnl_usd"],
            "roi_pct": pnl["roi_pct"],
            "close_reason": close_reason,
        }

        await self.db.close_position(position_id, exit_data)

        logger.info(
            f"📉 Closed position #{position_id} | "
            f"{position.symbol} | Exit spread: {exit_data['exit_spread_pct']:.4f}% | "
            f"Reason: {close_reason} | "
            f"P&L: ${exit_data['net_profit_usd']:.2f} ({exit_data['roi_pct']:.2f}%)"
        )

    def _calculate_pnl(self, position: Position, exit_spread: dict) -> dict:
        from .calculations import calculate_position_pnl, calculate_fees

        # Calculate actual exit fees using exit prices
        exit_fees = calculate_fees(
            quantity=position.quantity,
            long_price=exit_spread["exit_long_price"],
            short_price=exit_spread["exit_short_price"],
            long_fee_pct=exit_spread["long_fee_pct"],
            short_fee_pct=exit_spread["short_fee_pct"],
        )

        # Use centralized PnL calculation
        return calculate_position_pnl(
            quantity=position.quantity,
            entry_long_price=position.entry_long_price,
            entry_short_price=position.entry_short_price,
            exit_long_price=exit_spread["exit_long_price"],
            exit_short_price=exit_spread["exit_short_price"],
            entry_fees_usd=position.entry_fees_usd,
            exit_fees_usd=exit_fees,
            margin_used_usd=position.margin_used_usd,
        )

    async def close_position_partial(
        self,
        position: Position,
        close_quantity: float,
        exit_spread: dict,
        close_reason: str,
    ) -> bool:
        """Close portion of position and update tracking.

        Args:
            position: Position to partially close
            close_quantity: Quantity to close (must be <= remaining_quantity)
            exit_spread: Exit spread data with prices and fees
            close_reason: Reason for partial close

        Returns:
            True if position fully closed after this partial close, False if still open
        """
        # Initialize remaining_quantity if first partial close
        if position.remaining_quantity is None:
            position.remaining_quantity = position.quantity

        # Validate close quantity
        if close_quantity > position.remaining_quantity:
            logger.error(
                f"Cannot close {close_quantity:.6f} from position #{position.id} "
                f"(only {position.remaining_quantity:.6f} remaining)"
            )
            return False

        # Update remaining quantity
        position.remaining_quantity -= close_quantity

        # Calculate P&L for this chunk using proportion of original position
        chunk_pnl = self._calculate_pnl_for_quantity(
            position, exit_spread, close_quantity
        )

        # Accumulate P&L (don't overwrite existing partial close P&L)
        position.gross_profit_usd = (position.gross_profit_usd or 0) + chunk_pnl[
            "gross_pnl_usd"
        ]
        position.net_profit_usd = (position.net_profit_usd or 0) + chunk_pnl[
            "net_pnl_usd"
        ]
        position.exit_fees_usd = (position.exit_fees_usd or 0) + chunk_pnl[
            "exit_fees_usd"
        ]
        position.total_fees_usd = position.entry_fees_usd + (
            position.exit_fees_usd or 0
        )
        position.exit_long_price = exit_spread["exit_long_price"]
        position.exit_short_price = exit_spread["exit_short_price"]
        position.exit_spread_pct = chunk_pnl["exit_spread_pct"]
        position.roi_pct = (position.net_profit_usd / position.margin_used_usd) * 100  # type: ignore

        # Check if position is now fully closed (small threshold for floating point)
        fully_closed = position.remaining_quantity <= 0.0001

        if fully_closed:
            position.status = "closed"
            position.closed_at = datetime.now()
            position.close_reason = close_reason
            position.remaining_quantity = 0  # Set to exactly zero

            logger.info(
                f"✅ Fully closed position #{position.id} after partial closes | "
                f"{position.symbol} | "
                f"Final P&L: ${position.net_profit_usd:.2f} ({position.roi_pct:.2f}%)"
            )
        else:
            position.status = "partially_closed"
            logger.info(
                f"🔄 Partially closed position #{position.id} | "
                f"{position.symbol} | "
                f"Closed {close_quantity:.6f}, remaining {position.remaining_quantity:.6f} | "
                f"Partial P&L: ${chunk_pnl['net_pnl_usd']:.2f}"
            )

        # Save to database
        await self.db.update_position(position)

        return fully_closed

    def _calculate_pnl_for_quantity(
        self, position: Position, exit_spread: dict, quantity: float
    ) -> dict:
        """Calculate P&L for a specific quantity (used in partial closes)."""
        from .calculations import calculate_position_pnl, calculate_fees

        # Calculate fees proportional to the quantity being closed
        exit_fees = calculate_fees(
            quantity=quantity,
            long_price=exit_spread["exit_long_price"],
            short_price=exit_spread["exit_short_price"],
            long_fee_pct=exit_spread["long_fee_pct"],
            short_fee_pct=exit_spread["short_fee_pct"],
        )

        # Calculate entry fees proportional to this quantity
        entry_fees_proportional = position.entry_fees_usd * (
            quantity / position.quantity
        )

        # Calculate margin used for this quantity
        margin_proportional = position.margin_used_usd * (quantity / position.quantity)

        return calculate_position_pnl(
            quantity=quantity,
            entry_long_price=position.entry_long_price,
            entry_short_price=position.entry_short_price,
            exit_long_price=exit_spread["exit_long_price"],
            exit_short_price=exit_spread["exit_short_price"],
            entry_fees_usd=entry_fees_proportional,
            exit_fees_usd=exit_fees,
            margin_used_usd=margin_proportional,
        )

    async def handle_asymmetric_close(
        self,
        position: Position,
        long_closed: bool,
        short_closed: bool,
        exit_data: dict,
    ):
        """Handle scenario where one leg closes successfully but other fails.

        This is critical for risk management in paper trading simulation.
        In real trading, this would require immediate hedging action.

        Args:
            position: Position being closed
            long_closed: Whether long leg was successfully closed
            short_closed: Whether short leg was successfully closed
            exit_data: Exit prices and data
        """
        position.long_leg_closed = long_closed
        position.short_leg_closed = short_closed

        if long_closed and not short_closed:
            position.status = "partial_leg_closed"
            position.long_leg_closed_at = datetime.now()

            logger.warning(
                f"⚠️ ASYMMETRIC CLOSE: Position #{position.id} long leg closed, short leg still open! | "
                f"{position.symbol} {position.long_exchange}/{position.short_exchange} | "
                f"This represents UNHEDGED market risk exposure"
            )

        elif short_closed and not long_closed:
            position.status = "partial_leg_closed"
            position.short_leg_closed_at = datetime.now()

            logger.warning(
                f"⚠️ ASYMMETRIC CLOSE: Position #{position.id} short leg closed, long leg still open! | "
                f"{position.symbol} {position.long_exchange}/{position.short_exchange} | "
                f"This represents UNHEDGED market risk exposure"
            )

        elif long_closed and short_closed:
            position.status = "closed"
            position.closed_at = datetime.now()

            logger.info(
                f"✅ Both legs closed for position #{position.id} | {position.symbol}"
            )

        # Save updated position
        await self.db.update_position(position)

    async def record_close_attempt(
        self,
        position: Position,
        long_bid_qnt: float,
        short_ask_qnt: float,
        attempted_long_price: float,
        attempted_short_price: float,
        attempted_spread_pct: float,
        success: bool,
        failure_reason: Optional[str] = None,
        partial_close: bool = False,
        closed_quantity: Optional[float] = None,
    ) -> int:
        """Record a close attempt for audit trail.

        Args:
            position: Position being closed
            long_bid_qnt: Available bid quantity on long exchange
            short_ask_qnt: Available ask quantity on short exchange
            attempted_long_price: Bid price attempted for long leg
            attempted_short_price: Ask price attempted for short leg
            attempted_spread_pct: Spread at time of attempt
            success: Whether close was successful
            failure_reason: Why close failed (if applicable)
            partial_close: Whether this was a partial close
            closed_quantity: Quantity closed (if partial)

        Returns:
            ID of created close attempt record
        """
        required_qty = (
            position.remaining_quantity
            if position.remaining_quantity
            else position.quantity
        )

        close_attempt = CloseAttempt(
            position_id=position.id,  # pyright: ignore[reportArgumentType]
            long_bid_qnt=long_bid_qnt,
            short_ask_qnt=short_ask_qnt,
            required_qnt=required_qty,
            liquidity_sufficient=min(long_bid_qnt, short_ask_qnt) >= required_qty,
            attempted_long_price=attempted_long_price,
            attempted_short_price=attempted_short_price,
            attempted_spread_pct=attempted_spread_pct,
            success=success,
            failure_reason=failure_reason,
            partial_close=partial_close,
            closed_quantity=closed_quantity,
        )

        return await self.db.create_close_attempt(close_attempt)
