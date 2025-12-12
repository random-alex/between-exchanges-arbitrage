"""Utility functions for spread calculations."""

from datetime import datetime, timezone
from pathlib import Path
from app_ver2.connectors.models import Ticker
from app_ver2.instrument_fetcher import InstrumentFetcher, InstrumentSpec
from app_ver2.position_manager.calculations import calculate_fees
import aiofiles
import aiocsv


# Contract specifications are now fetched dynamically via InstrumentFetcher


def validate_and_adjust_quantity(
    quantity: float,
    buy_spec: InstrumentSpec,
    sell_spec: InstrumentSpec,
) -> float | None:
    """Validate and adjust order quantity to meet exchange requirements.

    Args:
        quantity: Desired order quantity (in base currency, e.g., BTC)
        buy_spec: Instrument spec for buy exchange
        sell_spec: Instrument spec for sell exchange

    Returns:
        Adjusted quantity that meets both exchanges' requirements, or None if impossible
    """
    # Check minimum order quantity for both exchanges
    max_min_qty = max(buy_spec.min_order_qnt, sell_spec.min_order_qnt)

    if quantity < max_min_qty:
        return None

    # Adjust to meet quantity step requirements (use larger step to satisfy both)
    step = max(buy_spec.qnt_step, sell_spec.qnt_step)

    # Round down to nearest valid step
    adjusted_quantity = (quantity // step) * step

    # Check if rounded quantity still meets minimum
    if adjusted_quantity < max_min_qty:
        return None

    return adjusted_quantity


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
    instrument_fetcher: InstrumentFetcher,  # InstrumentFetcher instance
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
    # Get contract specs from fetcher
    spec1 = instrument_fetcher.get_spec(t1.exchange, t1.instrument_id)
    spec2 = instrument_fetcher.get_spec(t2.exchange, t2.instrument_id)

    # Determine arbitrage direction and calculate relevant liquidity
    if t1.ask_price < t2.bid_price:
        # Buy t1, sell t2
        base_buy_price = t1.ask_price
        base_sell_price = t2.bid_price
        buy_exchange = t1.exchange
        sell_exchange = t2.exchange

        # Only check liquidity on sides we're actually trading
        buy_liquidity_usd = t1.ask_qnt * spec1.contract_size * t1.ask_price
        sell_liquidity_usd = t2.bid_qnt * spec2.contract_size * t2.bid_price
        liquidity = min(buy_liquidity_usd, sell_liquidity_usd)

    elif t2.ask_price < t1.bid_price:
        # Buy t2, sell t1
        base_buy_price = t2.ask_price
        base_sell_price = t1.bid_price
        buy_exchange = t2.exchange
        sell_exchange = t1.exchange

        # Only check liquidity on sides we're actually trading
        buy_liquidity_usd = t2.ask_qnt * spec2.contract_size * t2.ask_price
        sell_liquidity_usd = t1.bid_qnt * spec1.contract_size * t1.bid_price
        liquidity = min(buy_liquidity_usd, sell_liquidity_usd)

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

    # Calculate initial quantity
    initial_btc_amount = notional / buy_price

    # Determine which spec is buy and which is sell
    if buy_exchange == t1.exchange:
        buy_spec = spec1
        sell_spec = spec2
    else:
        buy_spec = spec2
        sell_spec = spec1

    # Validate and adjust quantity to meet exchange requirements
    btc_amount = validate_and_adjust_quantity(initial_btc_amount, buy_spec, sell_spec)

    if btc_amount is None:
        # Order size too small or doesn't meet requirements
        return None

    # Recalculate notional with adjusted quantity
    notional = btc_amount * buy_price

    # Calculate profit with adjusted values
    gross_profit = (sell_price - buy_price) * btc_amount

    # Calculate entry fees using actual prices (one-way only)
    entry_fees = calculate_fees(
        btc_amount, buy_price, sell_price, buy_spec.fee_pct, sell_spec.fee_pct
    )

    # For ROI estimation, assume similar exit fees
    estimated_exit_fees = entry_fees
    estimated_total_fees = entry_fees + estimated_exit_fees

    net_profit = gross_profit - estimated_total_fees
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
        "entry_fees_usd": entry_fees,  # One-way entry fees
        "estimated_total_fees_usd": estimated_total_fees,  # Estimated round-trip
        "net_profit_usd": net_profit,
        "roi_pct": roi,
        "is_profitable": net_profit > 0,
        "long_exchange": buy_exchange,  # Exchange where we're long
        "entry_long_price": buy_price,  # Entry price on long exchange
        "short_exchange": sell_exchange,  # Exchange where we're short
        "entry_short_price": sell_price,  # Entry price on short exchange
        "btc_amount": btc_amount,
        "initial_btc_amount": initial_btc_amount,
        "buy_min_qty": buy_spec.min_order_qnt,
        "sell_min_qty": sell_spec.min_order_qnt,
        "buy_qty_step": buy_spec.qnt_step,
        "sell_qty_step": sell_spec.qnt_step,
        "quantity_adjusted": abs(btc_amount - initial_btc_amount) > 1e-10,
        "notional_usd": notional,
        "margin_used_usd": margin_used,
        "liquidity_usd": liquidity,
        "leverage": leverage,
    }


async def log_to_csv_async(spread_data: dict, symbol: str = "arbitrage_data"):
    """Async CSV logging that doesn't block the event loop."""
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    filepath = data_dir / f"{symbol}.csv"

    # Define consistent field order
    base_fields = [
        "timestamp",
        "spread_pct",
        "gross_profit_usd",
        "total_fees_usd",
        "net_profit_usd",
        "roi_pct",
        "is_profitable",
        "buy_exchange",
        "buy_price",
        "sell_exchange",
        "sell_price",
        "btc_amount",
        "initial_btc_amount",
        "buy_min_qty",
        "sell_min_qty",
        "buy_qty_step",
        "sell_qty_step",
        "quantity_adjusted",
        "notional_usd",
        "margin_used_usd",
        "liquidity_usd",
        "leverage",
    ]

    exchange_fields = [k for k in spread_data.keys() if k.endswith(("_bid", "_ask"))]
    fieldnames = base_fields[:1] + sorted(exchange_fields) + base_fields[1:]

    file_exists = filepath.exists()

    async with aiofiles.open(filepath, "a", newline="") as f:
        writer = aiocsv.AsyncDictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            await writer.writeheader()
        await writer.writerow(spread_data)
