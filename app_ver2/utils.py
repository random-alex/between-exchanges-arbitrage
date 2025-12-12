"""Utility functions for spread calculations."""

from datetime import datetime, timezone
from pathlib import Path
from app_ver2.connectors.models import Ticker
from app_ver2.instrument_fetcher import InstrumentFetcher, InstrumentSpec
from app_ver2.position_manager.calculations import calculate_fees
from app_ver2.position_manager.models import Position
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


def validate_close_liquidity_level1(
    position: Position,
    long_ticker: Ticker,
    short_ticker: Ticker,
    long_spec: InstrumentSpec,
    short_spec: InstrumentSpec,
    min_liquidity_pct: float = 100.0,  # Require 100% of position qty available
) -> dict:
    """PHASE 1: Validates liquidity using only level 1 bid/ask quantities.

    To close a position:
    - Long leg (sell): Need to sell position.quantity on long exchange → check bid_qnt
    - Short leg (buy): Need to buy position.quantity on short exchange → check ask_qnt

    Args:
        position: Position to close
        long_ticker: Current ticker data for long exchange
        short_ticker: Current ticker data for short exchange
        long_spec: Instrument spec for long exchange
        short_spec: Instrument spec for short exchange
        min_liquidity_pct: Minimum % of position required to be available (default 100%)

    Returns:
        dict with:
        - can_close_full: bool - Full position closeable at level 1
        - can_close_partial: bool - Partial close possible (meets exchange min order size)
        - available_long_qty: float - Quantity available on long side (bid_qnt)
        - available_short_qty: float - Quantity available on short side (ask_qnt)
        - max_closeable_qty: float - Min of both sides
        - liquidity_ratio: float - Available / Required (0.0-1.0+)
        - closure_strategy: str - "full", "partial", "wait", or "force"
        - warning_message: Optional[str] - Human-readable warning if insufficient
        - long_ratio: float - Long side liquidity ratio
        - short_ratio: float - Short side liquidity ratio
    """
    # Position quantity that needs to be closed
    required_qty = (
        position.remaining_quantity
        if position.remaining_quantity
        else position.quantity
    )

    # Get available quantities from level 1
    # To close long position: sell into bids
    long_available_qty = long_ticker.bid_qnt

    # To close short position: buy from asks
    short_available_qty = short_ticker.ask_qnt

    # Determine what's closeable
    can_close_long = long_available_qty >= required_qty
    can_close_short = short_available_qty >= required_qty
    can_close_full = can_close_long and can_close_short

    # Calculate maximum closeable quantity (limited by less liquid side)
    max_closeable_qty = min(long_available_qty, short_available_qty)

    # Check if partial close meets exchange minimum order requirements
    min_order_qty = max(long_spec.min_order_qnt, short_spec.min_order_qnt)
    can_close_partial = (
        max_closeable_qty >= min_order_qty and max_closeable_qty < required_qty
    )

    # Calculate liquidity ratios
    long_ratio = long_available_qty / required_qty if required_qty > 0 else 1.0
    short_ratio = short_available_qty / required_qty if required_qty > 0 else 1.0
    liquidity_ratio = min(long_ratio, short_ratio)

    # Determine strategy
    if can_close_full:
        strategy = "full"
        warning = None
    elif can_close_partial:
        strategy = "partial"
        warning = f"Insufficient liquidity for full close. Available: {liquidity_ratio * 100:.1f}%"
    else:
        strategy = "wait"
        warning = f"Insufficient liquidity. Available: {max_closeable_qty:.6f} (min required: {min_order_qty:.6f})"

    return {
        "can_close_full": can_close_full,
        "can_close_partial": can_close_partial,
        "available_long_qty": long_available_qty,
        "available_short_qty": short_available_qty,
        "max_closeable_qty": max_closeable_qty,
        "liquidity_ratio": liquidity_ratio,
        "closure_strategy": strategy,
        "warning_message": warning,
        "long_ratio": long_ratio,
        "short_ratio": short_ratio,
    }


def estimate_slippage_simple(position_qty: float, available_qty: float) -> float:
    """PHASE 1: Simple slippage estimation without orderbook depth.

    This is a heuristic-based approach that estimates slippage when closing
    a position larger than the available level 1 liquidity.

    Heuristic:
    - If qty <= available: 0% additional slippage (filled at best price)
    - If qty > available: 0.1% slippage per 10% overage

    Example: If position is 150 BTC and only 100 available:
    - Overage: 50% (50 BTC / 100 BTC)
    - Estimated slippage: 0.5% (50% * 0.01)

    Phase 2 will replace this with multi-level orderbook walking for accuracy.

    Args:
        position_qty: Quantity to close
        available_qty: Quantity available at level 1

    Returns:
        Estimated slippage percentage (e.g., 0.5 = 0.5%)
    """
    if position_qty <= available_qty:
        return 0.0

    # Calculate overage percentage
    overage_pct = ((position_qty - available_qty) / available_qty) * 100

    # 0.1% slippage per 10% overage
    slippage_pct = overage_pct * 0.01

    # Cap at 2% to avoid unrealistic estimates
    return min(slippage_pct, 2.0)


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
