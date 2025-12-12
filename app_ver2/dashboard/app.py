import streamlit as st
from datetime import datetime
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from position_manager.database import PositionDB


st.set_page_config(page_title="Arbitrage Monitor", page_icon="📊", layout="wide")

DB_PATH = Path(__file__).parent.parent.parent / "data" / "positions.db"


@st.cache_resource
def get_db():
    return PositionDB(str(DB_PATH))


async def load_stats():
    db = get_db()
    return await db.get_position_stats()


async def load_open_positions():
    db = get_db()
    return await db.get_open_positions()


async def load_closed_positions(limit=20):
    db = get_db()
    return await db.get_closed_positions(limit)


async def load_close_attempts(position_id: int, limit=10):
    db = get_db()
    return await db.get_close_attempts(position_id, limit)


def main():
    st.title("📊 Arbitrage Monitor")

    try:
        stats = asyncio.run(load_stats())
        open_positions = asyncio.run(load_open_positions())
        closed_positions = asyncio.run(load_closed_positions(20))
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Open Positions", stats["open_positions"])

    with col2:
        st.metric("Closed Positions", stats["closed_positions"])

    with col3:
        pnl = stats["total_pnl_usd"]
        st.metric("Total P&L", f"${pnl:.2f}", delta=f"{pnl:.2f}")

    with col4:
        st.metric("Win Rate", f"{stats['win_rate_pct']:.1f}%")

    col5, col6 = st.columns(2)

    with col5:
        st.metric("Avg ROI", f"{stats['avg_roi_pct']:.2f}%")

    # Alerts Section - Show positions with issues
    if open_positions:
        stuck_positions = [
            p
            for p in open_positions
            if p.close_attempts >= 5 or p.close_liquidity_warnings >= 3
        ]

        if stuck_positions:
            st.divider()
            st.warning(f"⚠️ **{len(stuck_positions)} Position(s) with Issues**")

            for p in stuck_positions:
                alert_messages = []
                if p.close_attempts >= 5:
                    alert_messages.append(f"{p.close_attempts} close attempts")
                if p.close_liquidity_warnings >= 3:
                    alert_messages.append(
                        f"{p.close_liquidity_warnings} liquidity warnings"
                    )

                hold_hours = (datetime.now() - p.created_at).total_seconds() / 3600
                st.error(
                    f"Position #{p.id} - {p.symbol} ({p.long_exchange}/{p.short_exchange}) | "
                    f"{', '.join(alert_messages)} | Open for {hold_hours:.1f}h"
                )

    st.divider()

    st.subheader("🔥 Open Positions")

    if not open_positions:
        st.info("No open positions")
    else:
        for p in open_positions:
            hold_hours = (datetime.now() - p.created_at).total_seconds() / 3600
            remaining = p.remaining_quantity if p.remaining_quantity else p.quantity
            status_emoji = "🟢" if p.status == "open" else "🟡"

            # Create main position row
            col_a, col_b, col_c, col_d = st.columns([1, 2, 2, 1])

            with col_a:
                st.markdown(f"**{status_emoji} #{p.id}**")
                st.caption(f"{p.symbol}")

            with col_b:
                st.metric("Entry Spread", f"{p.entry_spread_pct:.4f}%")
                st.caption(f"{p.long_exchange} → {p.short_exchange}")

            with col_c:
                if p.status == "partially_closed":
                    closed_qty = p.quantity - remaining
                    st.metric("Position", f"{remaining:.4f} / {p.quantity:.4f}")
                    st.caption(
                        f"Closed: {closed_qty:.4f} ({(closed_qty / p.quantity) * 100:.1f}%)"
                    )
                else:
                    st.metric("Quantity", f"{remaining:.4f}")
                    st.caption(f"Notional: ${p.notional_usd:.2f}")

            with col_d:
                st.metric("Hold Time", f"{hold_hours:.1f}h")
                if p.close_attempts > 0:
                    st.caption(f"⚠️ {p.close_attempts} close attempts")

            # Expandable details
            with st.expander(f"Details for position #{p.id}"):
                detail_col1, detail_col2, detail_col3 = st.columns(3)

                with detail_col1:
                    st.markdown("**Entry Details**")
                    st.text(f"Long Price: {p.entry_long_price:.4f}")
                    st.text(f"Short Price: {p.entry_short_price:.4f}")
                    st.text(f"Entry Fees: ${p.entry_fees_usd:.2f}")

                with detail_col2:
                    st.markdown("**Close Tracking**")
                    st.text(f"Attempts: {p.close_attempts}")
                    st.text(f"Liquidity Warnings: {p.close_liquidity_warnings}")
                    if p.actual_slippage_estimate:
                        st.text(f"Est. Slippage: ${p.actual_slippage_estimate:.2f}")
                    if p.first_close_attempt_at:
                        st.text(
                            f"First Attempt: {p.first_close_attempt_at.strftime('%H:%M:%S')}"
                        )

                with detail_col3:
                    st.markdown("**Leg Status**")
                    long_status = "✅ Closed" if p.long_leg_closed else "🔴 Open"
                    short_status = "✅ Closed" if p.short_leg_closed else "🔴 Open"
                    st.text(f"Long Leg: {long_status}")
                    st.text(f"Short Leg: {short_status}")
                    if p.status == "partially_closed":
                        st.text(
                            f"Partial P&L: ${p.net_profit_usd:.2f}"
                            if p.net_profit_usd
                            else "Partial P&L: $0.00"
                        )

                # Close attempts history
                if p.close_attempts > 0:
                    st.markdown("**Close Attempt History**")
                    try:
                        close_attempts = asyncio.run(
                            load_close_attempts(p.id, limit=10)
                        )

                        if close_attempts:
                            attempt_data = []
                            for attempt in close_attempts:
                                success_icon = "✅" if attempt.success else "❌"
                                reason = attempt.failure_reason or "Success"

                                attempt_data.append(
                                    {
                                        "Time": attempt.attempted_at.strftime(
                                            "%H:%M:%S"
                                        ),
                                        "Status": success_icon,
                                        "Long Bid Qty": f"{attempt.long_bid_qnt:.4f}",
                                        "Short Ask Qty": f"{attempt.short_ask_qnt:.4f}",
                                        "Required": f"{attempt.required_qnt:.4f}",
                                        "Sufficient": "✅"
                                        if attempt.liquidity_sufficient
                                        else "❌",
                                        "Spread %": f"{attempt.attempted_spread_pct:.4f}",
                                        "Reason": reason,
                                    }
                                )

                            st.dataframe(attempt_data, hide_index=True)
                        else:
                            st.caption("No close attempts recorded")
                    except Exception as e:
                        st.error(f"Error loading close attempts: {e}")

            st.divider()

    st.divider()

    st.subheader("📈 Recent Closed Positions")

    if not closed_positions:
        st.info("No closed positions")
    else:
        closed_data = []
        for p in closed_positions:
            hold_hours = (
                (p.closed_at - p.created_at).total_seconds() / 3600
                if p.closed_at
                else 0
            )

            # Add indicator for partial closes or difficult closes
            status_indicators = []
            if p.remaining_quantity and p.remaining_quantity < p.quantity:
                status_indicators.append("🟡Partial")
            if p.close_attempts > 3:
                status_indicators.append(f"⚠️{p.close_attempts}x")
            if p.actual_slippage_estimate and p.actual_slippage_estimate > 10:
                status_indicators.append("💸Slippage")

            status = " ".join(status_indicators) if status_indicators else ""

            closed_data.append(
                {
                    "ID": f"#{p.id}",
                    "Status": status,
                    "Symbol": p.symbol,
                    "Long": p.long_exchange,
                    "Short": p.short_exchange,
                    "Entry %": f"{p.entry_spread_pct:.4f}",
                    "Exit %": f"{p.exit_spread_pct:.4f}" if p.exit_spread_pct else "0",
                    "P&L": f"${p.net_profit_usd:.2f}" if p.net_profit_usd else "$0.00",
                    "ROI %": f"{p.roi_pct:.2f}" if p.roi_pct else "0.00",
                    "Qnt": f"{p.quantity:.2f}",
                    "Hold": f"{hold_hours:.1f}h",
                    "Attempts": p.close_attempts,
                    "Reason": p.close_reason or "-",
                }
            )

        st.dataframe(closed_data, hide_index=True, use_container_width=True)

    st.caption(f"Last update: {datetime.now().strftime('%H:%M:%S')}")

    st.button("🔄 Refresh")


if __name__ == "__main__":
    main()
