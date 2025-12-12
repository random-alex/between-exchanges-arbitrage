import logging
from datetime import datetime
from typing import Optional

from .models import Position
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
