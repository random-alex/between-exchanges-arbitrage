from sqlmodel import SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from pathlib import Path
from typing import Optional
import logging

from .models import Position

logger = logging.getLogger(__name__)


class PositionDB:
    def __init__(self, db_path: str = "data/positions.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True)

        db_url = f"sqlite+aiosqlite:///{self.db_path}"
        self.engine = create_async_engine(
            db_url, echo=False, connect_args={"check_same_thread": False}
        )

    async def initialize(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)
        logger.info(f"Initialized database at {self.db_path}")

    async def create_position(self, position: Position) -> int:
        async with AsyncSession(self.engine) as session:
            session.add(position)
            await session.commit()
            await session.refresh(position)
            return position.id  # pyright: ignore[reportReturnType]

    async def get_open_positions(self) -> list[Position]:
        async with AsyncSession(self.engine) as session:
            result = await session.exec(
                select(Position).where(Position.status == "open")
            )
            return list(result.all())

    async def has_open_position_for_symbol(self, symbol: str) -> bool:
        async with AsyncSession(self.engine) as session:
            result = await session.exec(
                select(Position).where(
                    Position.symbol == symbol, Position.status == "open"
                )
            )
            return result.first() is not None

    async def has_open_position_for_symbol_and_exchanges(
        self, symbol: str, exchange1: str, exchange2: str
    ) -> bool:
        """Check if either exchange is already used in an open position for this symbol"""
        async with AsyncSession(self.engine) as session:
            from sqlalchemy import or_

            result = await session.exec(
                select(Position).where(
                    Position.symbol == symbol,
                    Position.status == "open",
                    or_(
                        Position.buy_exchange == exchange1,
                        Position.buy_exchange == exchange2,
                        Position.sell_exchange == exchange1,
                        Position.sell_exchange == exchange2,
                    ),
                )
            )
            return result.first() is not None

    async def get_position(self, position_id: int) -> Optional[Position]:
        async with AsyncSession(self.engine) as session:
            return await session.get(Position, position_id)

    async def close_position(self, position_id: int, exit_data: dict):
        async with AsyncSession(self.engine) as session:
            position = await session.get(Position, position_id)
            if not position:
                raise ValueError(f"Position {position_id} not found")

            from datetime import datetime

            position.status = "closed"
            position.closed_at = datetime.now()
            position.exit_buy_price = exit_data["exit_buy_price"]
            position.exit_sell_price = exit_data["exit_sell_price"]
            position.exit_spread_pct = exit_data["exit_spread_pct"]
            position.gross_profit_usd = exit_data["gross_profit_usd"]
            position.fees_usd = exit_data["fees_usd"]
            position.net_profit_usd = exit_data["net_profit_usd"]
            position.roi_pct = exit_data["roi_pct"]
            position.close_reason = exit_data["close_reason"]

            session.add(position)
            await session.commit()

    async def get_position_stats(self) -> dict:
        async with AsyncSession(self.engine) as session:
            open_result = await session.execute(
                select(Position).where(Position.status == "open")
            )
            open_count = len(list(open_result.scalars().all()))

            closed_result = await session.execute(
                select(Position).where(Position.status == "closed")
            )
            closed_positions = list(closed_result.scalars().all())
            closed_count = len(closed_positions)

            total_pnl = sum(p.net_profit_usd or 0 for p in closed_positions)
            wins = sum(1 for p in closed_positions if (p.net_profit_usd or 0) > 0)
            win_rate = (wins / closed_count * 100) if closed_count > 0 else 0

            avg_roi = 0
            if closed_count > 0:
                roi_sum = sum(p.roi_pct or 0 for p in closed_positions)
                avg_roi = roi_sum / closed_count

            return {
                "open_positions": open_count,
                "closed_positions": closed_count,
                "total_pnl_usd": total_pnl,
                "win_rate_pct": win_rate,
                "avg_roi_pct": avg_roi,
            }

    async def close(self):
        await self.engine.dispose()
