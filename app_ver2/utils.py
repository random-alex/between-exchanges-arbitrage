"""Utility functions for spread calculations."""

from datetime import datetime, timezone
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from app.connectors.models import Ticker


# Exchange contract specifications
CONTRACT_SPECS = {
    "okx": {"size_btc": 0.01, "fee_pct": 0.05},
    "bybit": {"size_btc": 1.0, "fee_pct": 0.1},
    "binance": {"size_btc": 1.0, "fee_pct": 0.05},
    "deribit": {"size_btc": 1.0, "fee_pct": 0.05},
}


def calculate_dynamic_slippage(notional: float, liquidity: float) -> float:
    """Calculate slippage based on order size vs available liquidity.

    Args:
        notional: Order size in USD
        liquidity: Available liquidity in USD

    Returns:
        Slippage in percentage (e.g., 0.05 = 0.05%)
    """
    if liquidity == 0:
        return 0.5  # High slippage if no liquidity

    size_ratio = notional / liquidity

    if size_ratio < 0.01:  # Less than 1% of liquidity
        return 0.01  # 1 basis point
    elif size_ratio < 0.05:  # Less than 5% of liquidity
        return 0.05  # 5 bps
    elif size_ratio < 0.10:  # Less than 10% of liquidity
        return 0.10  # 10 bps
    else:
        # Higher impact for larger orders
        return min(0.20 + (size_ratio - 0.10) * 2, 0.50)


def calculate_spread(
    t1: Ticker,
    t2: Ticker,
    capital: float = 100,
    leverage: float = 10.0,
    slippage: float | None = None,  # If None, use dynamic calculation
    min_spread_threshold: float = 0.15,  # Minimum spread to consider (%)
) -> dict | None:
    """Calculate arbitrage spread between two tickers.

    Args:
        t1, t2: Ticker objects from two exchanges
        capital: Available capital in USD
        leverage: Leverage multiplier
        slippage: Fixed slippage % (if None, calculates dynamically)
        min_spread_threshold: Minimum spread % to consider profitable

    Returns:
        Dict with spread data or None if no arbitrage
    """
    # Get contract specs
    spec1 = CONTRACT_SPECS.get(t1.exchange)
    spec2 = CONTRACT_SPECS.get(t2.exchange)

    if not spec1 or not spec2:
        return None

    # Determine arbitrage direction and calculate relevant liquidity
    if t1.ask_price < t2.bid_price:
        # Buy t1, sell t2
        base_buy_price = t1.ask_price
        base_sell_price = t2.bid_price
        buy_exchange = t1.exchange
        sell_exchange = t2.exchange

        # Only check liquidity on sides we're actually trading
        buy_liquidity_usd = t1.ask_qnt * spec1["size_btc"] * t1.ask_price
        sell_liquidity_usd = t2.bid_qnt * spec2["size_btc"] * t2.bid_price
        liquidity = min(buy_liquidity_usd, sell_liquidity_usd)

        fee_pct = spec1["fee_pct"] + spec2["fee_pct"]

    elif t2.ask_price < t1.bid_price:
        # Buy t2, sell t1
        base_buy_price = t2.ask_price
        base_sell_price = t1.bid_price
        buy_exchange = t2.exchange
        sell_exchange = t1.exchange

        # Only check liquidity on sides we're actually trading
        buy_liquidity_usd = t2.ask_qnt * spec2["size_btc"] * t2.ask_price
        sell_liquidity_usd = t1.bid_qnt * spec1["size_btc"] * t1.bid_price
        liquidity = min(buy_liquidity_usd, sell_liquidity_usd)

        fee_pct = spec2["fee_pct"] + spec1["fee_pct"]

    else:
        return None  # No arbitrage opportunity

    # Check if spread is worth considering (before fees/slippage)
    raw_spread_pct = ((base_sell_price - base_buy_price) / base_buy_price) * 100
    if raw_spread_pct < min_spread_threshold:
        return None

    # Calculate position size
    max_notional = (capital / 2) * leverage
    notional = min(max_notional, liquidity)

    # Calculate or use provided slippage
    if slippage is None:
        slippage_pct = calculate_dynamic_slippage(notional, liquidity)
    else:
        slippage_pct = slippage

    # Apply slippage to prices
    buy_price = base_buy_price * (1 + slippage_pct / 100)
    sell_price = base_sell_price * (1 - slippage_pct / 100)

    # Calculate profit
    btc_amount = notional / buy_price
    gross_profit = (sell_price - buy_price) * btc_amount

    # Round-trip fees (open + close positions)
    total_fees = notional * fee_pct / 100 * 2

    net_profit = gross_profit - total_fees
    margin_used = (notional / leverage) * 2
    roi = (net_profit / margin_used) * 100
    spread_pct = ((sell_price - buy_price) / buy_price) * 100

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        f"{t1.exchange}_bid": t1.bid_price,
        f"{t1.exchange}_ask": t1.ask_price,
        f"{t2.exchange}_bid": t2.bid_price,
        f"{t2.exchange}_ask": t2.ask_price,
        "raw_spread_pct": raw_spread_pct,
        "spread_pct": spread_pct,
        "slippage_pct": slippage_pct,
        "gross_profit_usd": gross_profit,
        "total_fees_usd": total_fees,
        "net_profit_usd": net_profit,
        "roi_pct": roi,
        "is_profitable": net_profit > 0,
        "buy_exchange": buy_exchange,
        "buy_price": buy_price,
        "sell_exchange": sell_exchange,
        "sell_price": sell_price,
        "btc_amount": btc_amount,
        "notional_usd": notional,
        "margin_used_usd": margin_used,
        "liquidity_usd": liquidity,
        "leverage": leverage,
    }
