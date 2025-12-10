import streamlit as st
from datetime import datetime
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from position_manager import PositionDB


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
    from sqlmodel import select
    from sqlmodel.ext.asyncio.session import AsyncSession
    from position_manager.models import Position

    async with AsyncSession(db.engine) as session:
        result = await session.exec(
            select(Position)
            .where(Position.status == "closed")
            .order_by(Position.closed_at.desc())
            .limit(limit)
        )
        return list(result.all())


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

    st.divider()

    st.subheader("🔥 Open Positions")

    if not open_positions:
        st.info("No open positions")
    else:
        open_data = []
        for p in open_positions:
            hold_hours = (datetime.now() - p.created_at).total_seconds() / 3600
            open_data.append(
                {
                    "ID": f"#{p.id}",
                    "Symbol": p.symbol,
                    "Buy": p.buy_exchange,
                    "Sell": p.sell_exchange,
                    "Entry Spread %": f"{p.entry_spread_pct:.4f}",
                    "Quantity": f"{p.quantity:.6f}",
                    "Notional": f"${p.notional_usd:.2f}",
                    "Hold Time": f"{hold_hours:.1f}h",
                }
            )

        st.dataframe(open_data, width="content", hide_index=True)

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
            closed_data.append(
                {
                    "ID": f"#{p.id}",
                    "Symbol": p.symbol,
                    "Buy": p.buy_exchange,
                    "Sell": p.sell_exchange,
                    "Entry %": f"{p.entry_spread_pct:.4f}",
                    "Exit %": f"{p.exit_spread_pct:.4f}" if p.exit_spread_pct else "0",
                    "P&L": f"${p.net_profit_usd:.2f}" if p.net_profit_usd else "$0.00",
                    "ROI %": f"{p.roi_pct:.2f}" if p.roi_pct else "0.00",
                    "Hold": f"{hold_hours:.1f}h",
                    "Reason": p.close_reason or "-",
                }
            )

        st.dataframe(closed_data, width="content", hide_index=True)

    st.caption(f"Last update: {datetime.now().strftime('%H:%M:%S')}")

    st.button("🔄 Refresh")


if __name__ == "__main__":
    main()
