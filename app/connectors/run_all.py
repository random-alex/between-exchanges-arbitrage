from app.connectors.bybit import run_bybit, BYBIT_STREAM
from app.connectors.okx_connector import run_okx, OKX_STREAM
import asyncio
from app.connectors.models import Ticker
import csv
from datetime import datetime, timezone
from pathlib import Path


# In your spread_monitor function:
async def spread_monitor():
    print("🔍 Spread monitor started...")

    while True:
        await asyncio.sleep(2)

        if not BYBIT_STREAM or not OKX_STREAM:
            continue
        for key in BYBIT_STREAM.keys():
            if key not in OKX_STREAM.keys():
                continue

            bybit_ticker = BYBIT_STREAM[key]
            okx_ticker = OKX_STREAM[key]

            if (
                bybit_ticker.normilized_intrument_id
                != okx_ticker.normilized_intrument_id
            ):
                continue

            spread = calculate_spread(
                bybit_ticker, okx_ticker, available_capital_usd=100, leverage=10
            )
            if spread:
                # Log every non None calculation
                log_to_csv(spread, symbol=key)

                # Print opportunities
                if spread["is_profitable"] and spread["roi_pct"] > 0.5:
                    print(
                        f"🔥 {spread['roi_pct']:.2f}% ROI | "
                        f"Spread: {spread['spread_pct']:.3f}% | "
                        f"Profit: ${spread['net_profit_usd']:.2f}"
                    )


def calculate_spread(
    ticker1: Ticker,
    ticker2: Ticker,
    available_capital_usd: float = 100,
    leverage: float = 10.0,
    slippage_pct: float = 0.02,
) -> dict | None:
    contract_specs = {
        "okx": {"contract_size_btc": 0.01, "fee_pct": 0.05},
        "bybit": {"contract_size_btc": 1.0, "fee_pct": 0.1},
    }

    margin_per_side = available_capital_usd / 2
    notional_per_side = margin_per_side * leverage

    t1_spec = contract_specs[ticker1.exchange]
    t2_spec = contract_specs[ticker2.exchange]

    t1_ask_liquidity_usd = (
        ticker1.ask_qnt * t1_spec["contract_size_btc"] * ticker1.ask_price
    )
    t1_bid_liquidity_usd = (
        ticker1.bid_qnt * t1_spec["contract_size_btc"] * ticker1.bid_price
    )
    t2_ask_liquidity_usd = (
        ticker2.ask_qnt * t2_spec["contract_size_btc"] * ticker2.ask_price
    )
    t2_bid_liquidity_usd = (
        ticker2.bid_qnt * t2_spec["contract_size_btc"] * ticker2.bid_price
    )

    min_liquidity = min(
        t1_ask_liquidity_usd,
        t1_bid_liquidity_usd,
        t2_ask_liquidity_usd,
        t2_bid_liquidity_usd,
    )

    actual_notional = min(notional_per_side, min_liquidity)
    total_margin_used = (actual_notional / leverage) * 2

    if ticker1.ask_price < ticker2.bid_price:
        buy_price = ticker1.ask_price * (1 + slippage_pct / 100)
        sell_price = ticker2.bid_price * (1 - slippage_pct / 100)
        buy_exchange = ticker1.exchange
        sell_exchange = ticker2.exchange
        buy_fee_pct = t1_spec["fee_pct"]
        sell_fee_pct = t2_spec["fee_pct"]

    elif ticker2.ask_price < ticker1.bid_price:
        buy_price = ticker2.ask_price * (1 + slippage_pct / 100)
        sell_price = ticker1.bid_price * (1 - slippage_pct / 100)
        buy_exchange = ticker2.exchange
        sell_exchange = ticker1.exchange
        buy_fee_pct = t2_spec["fee_pct"]
        sell_fee_pct = t1_spec["fee_pct"]

    else:
        return None

    # Calculate profit assuming convergence
    btc_amount = actual_notional / buy_price
    gross_profit = (sell_price - buy_price) * btc_amount

    # All fees (entry + exit)
    entry_fees = actual_notional * (buy_fee_pct + sell_fee_pct) / 100
    exit_fees = actual_notional * (buy_fee_pct + sell_fee_pct) / 100
    total_fees = entry_fees + exit_fees

    net_profit_usd = gross_profit - total_fees
    roi_pct = (net_profit_usd / total_margin_used) * 100
    spread_pct = ((sell_price - buy_price) / buy_price) * 100

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "bybit_bid": ticker1.bid_price
        if ticker1.exchange == "bybit"
        else ticker2.bid_price,
        "bybit_ask": ticker1.ask_price
        if ticker1.exchange == "bybit"
        else ticker2.ask_price,
        "okx_bid": ticker1.bid_price
        if ticker1.exchange == "okx"
        else ticker2.bid_price,
        "okx_ask": ticker1.ask_price
        if ticker1.exchange == "okx"
        else ticker2.ask_price,
        "spread_pct": spread_pct,
        "gross_profit_usd": gross_profit,
        "total_fees_usd": total_fees,
        "net_profit_usd": net_profit_usd,
        "roi_pct": roi_pct,
        "is_profitable": net_profit_usd > 0,
        "buy_exchange": buy_exchange,
        "buy_price": buy_price,
        "sell_exchange": sell_exchange,
        "sell_price": sell_price,
        "btc_amount": btc_amount,
        "notional_usd": actual_notional,
        "margin_used_usd": total_margin_used,
        "liquidity_usd": min_liquidity,
        "leverage": leverage,
    }


def log_to_csv(spread_data: dict, symbol: str = "arbitrage_data"):
    """Append spread calculation to CSV file"""

    filepath = f"data/{symbol}.csv"
    file_exists = Path(filepath).exists()

    with open(filepath, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=spread_data.keys())

        if not file_exists:
            writer.writeheader()

        writer.writerow(spread_data)


async def main():
    # Run all three tasks in parallel
    await asyncio.gather(run_okx(), run_bybit(), spread_monitor())


if __name__ == "__main__":
    asyncio.run(main())
