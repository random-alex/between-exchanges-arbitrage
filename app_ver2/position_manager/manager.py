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
            if spread["buy_exchange"] == ticker1.exchange
            else ticker2.instrument_id
        )
        sell_instrument = (
            ticker1.instrument_id
            if spread["sell_exchange"] == ticker1.exchange
            else ticker2.instrument_id
        )

        position = Position(
            symbol=symbol,
            buy_exchange=spread["buy_exchange"],
            buy_instrument=buy_instrument,
            sell_exchange=spread["sell_exchange"],
            sell_instrument=sell_instrument,
            entry_buy_price=spread["buy_price"],
            entry_sell_price=spread["sell_price"],
            entry_spread_pct=spread["spread_pct"],
            quantity=spread["btc_amount"],
            notional_usd=spread["notional_usd"],
            margin_used_usd=spread["margin_used_usd"],
            entry_fees_usd=spread["total_fees_usd"]
            / 2,  # Entry fees only (half of round-trip)
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
            f"{symbol} | {spread['buy_exchange']}/{spread['sell_exchange']} | "
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
        exit_spread: Optional[dict],
        close_reason: str,
    ):
        if exit_spread is None:
            exit_data = {
                "exit_buy_price": position.entry_buy_price,
                "exit_sell_price": position.entry_sell_price,
                "exit_spread_pct": position.entry_spread_pct,
                "gross_profit_usd": 0.0,
                "fees_usd": 0.0,
                "net_profit_usd": 0.0,
                "roi_pct": 0.0,
                "close_reason": close_reason,
            }
        else:
            pnl = self._calculate_pnl(position, exit_spread)
            exit_data = {
                "exit_buy_price": exit_spread["exit_price_on_buy_exchange"],
                "exit_sell_price": exit_spread["exit_price_on_sell_exchange"],
                "exit_spread_pct": exit_spread["spread_pct"],
                "gross_profit_usd": pnl["gross_profit_usd"],
                "fees_usd": pnl["fees_usd"],
                "net_profit_usd": pnl["net_profit_usd"],
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
        quantity = position.quantity

        # Calculate P&L for each leg separately
        # Buy exchange leg: bought at entry_buy_price, sell at current bid
        buy_exchange_pnl = (
            exit_spread["exit_price_on_buy_exchange"] - position.entry_buy_price
        ) * quantity

        # Sell exchange leg: sold at entry_sell_price, buy back at current ask
        sell_exchange_pnl = (
            position.entry_sell_price - exit_spread["exit_price_on_sell_exchange"]
        ) * quantity

        # Gross P&L is sum of both legs
        gross_pnl = buy_exchange_pnl + sell_exchange_pnl

        # Calculate exit fees from exit_spread
        exit_fees = exit_spread.get("total_fees_usd", 0)

        # Total fees = entry fees (stored) + exit fees
        entry_fees = position.entry_fees_usd
        total_fees = entry_fees + exit_fees

        # Net P&L
        net_pnl = gross_pnl - total_fees
        roi = (net_pnl / position.margin_used_usd) * 100

        return {
            "gross_profit_usd": gross_pnl,
            "fees_usd": total_fees,
            "net_profit_usd": net_pnl,
            "roi_pct": roi,
        }
