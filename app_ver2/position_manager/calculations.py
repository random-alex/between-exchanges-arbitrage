"""Centralized PnL and fee calculations for arbitrage positions."""


def calculate_fees(
    quantity: float,
    long_price: float,
    short_price: float,
    long_fee_pct: float,
    short_fee_pct: float,
) -> float:
    """Calculate one-way fees for entry or exit.

    Fees are calculated separately for each leg based on actual notional
    values, then summed.

    Args:
        quantity: Position quantity (in base currency)
        long_price: Price on long exchange
        short_price: Price on short exchange
        long_fee_pct: Fee percentage on long exchange
        short_fee_pct: Fee percentage on short exchange

    Returns:
        Total fees in USD (one-way only, both legs combined)
    """
    # Calculate notional for each leg separately
    long_notional = quantity * long_price
    short_notional = quantity * short_price

    # Calculate fees per leg
    long_fee = long_notional * long_fee_pct / 100
    short_fee = short_notional * short_fee_pct / 100

    # Return sum of both fees
    return long_fee + short_fee


def calculate_leg_pnl(
    quantity: float,
    entry_price: float,
    exit_price: float,
    is_long: bool,
) -> float:
    """Calculate P&L for one leg (long or short).

    Args:
        quantity: Position quantity
        entry_price: Entry price for this leg
        exit_price: Exit price for this leg
        is_long: True if long leg, False if short leg

    Returns:
        P&L in USD for this leg (before fees)
    """
    if is_long:
        # Long: buy at entry, sell at exit
        return (exit_price - entry_price) * quantity
    else:
        # Short: sell at entry, buy back at exit
        return (entry_price - exit_price) * quantity


def calculate_position_pnl(
    quantity: float,
    entry_long_price: float,
    entry_short_price: float,
    exit_long_price: float,
    exit_short_price: float,
    entry_fees_usd: float,
    exit_fees_usd: float,
    margin_used_usd: float,
) -> dict:
    """Calculate complete P&L for closed position.

    Args:
        quantity: Position quantity
        entry_long_price: Entry price on long exchange
        entry_short_price: Entry price on short exchange
        exit_long_price: Exit price on long exchange
        exit_short_price: Exit price on short exchange
        entry_fees_usd: Fees paid at entry
        exit_fees_usd: Fees paid at exit
        margin_used_usd: Margin used for position

    Returns:
        dict with complete P&L breakdown:
            - exit_fees_usd: Exit fees
            - gross_pnl_usd: P&L before fees
            - net_pnl_usd: P&L after fees
            - roi_pct: Return on margin
            - exit_spread_pct: Spread at exit
    """
    # Calculate P&L for each leg
    long_leg_pnl = calculate_leg_pnl(
        quantity, entry_long_price, exit_long_price, is_long=True
    )
    short_leg_pnl = calculate_leg_pnl(
        quantity, entry_short_price, exit_short_price, is_long=False
    )

    gross_pnl = long_leg_pnl + short_leg_pnl

    # Total fees
    total_fees = entry_fees_usd + exit_fees_usd

    # Net P&L
    net_pnl = gross_pnl - total_fees

    # ROI
    roi_pct = (net_pnl / margin_used_usd) * 100 if margin_used_usd > 0 else 0

    # Exit spread percentage
    exit_spread_pct = (exit_long_price - exit_short_price) / exit_short_price * 100

    return {
        "exit_fees_usd": exit_fees_usd,
        "gross_pnl_usd": gross_pnl,
        "net_pnl_usd": net_pnl,
        "roi_pct": roi_pct,
        "exit_spread_pct": exit_spread_pct,
        "total_fees_usd": total_fees,
    }
