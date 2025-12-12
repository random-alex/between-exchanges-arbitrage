from datetime import datetime
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager
import logging

from sqlmodel import SQLModel, select, func, or_
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy.pool import StaticPool
from sqlalchemy import text

from .models import Position, CloseAttempt

logger = logging.getLogger(__name__)


class PositionDB:
    """Efficient async database for position management."""

    def __init__(self, db_path: str = "data/positions.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True)

        db_url = f"sqlite+aiosqlite:///{self.db_path}"

        # Optimized engine configuration
        self.engine: AsyncEngine = create_async_engine(
            db_url,
            echo=False,
            future=True,
            pool_pre_ping=True,
            poolclass=StaticPool,  # Efficient for SQLite
            connect_args={
                "check_same_thread": False,
                "timeout": 30,
            },
        )

    @asynccontextmanager
    async def session(self):
        """Reusable session context manager for batch operations."""
        async with AsyncSession(self.engine, expire_on_commit=False) as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def initialize(self):
        """Initialize database with WAL mode for better concurrency."""
        async with self.engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)
            # Enable Write-Ahead Logging for concurrent reads during writes
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.execute(text("PRAGMA synchronous=NORMAL"))
        logger.info(f"Initialized database with WAL mode at {self.db_path}")

    async def create_position(self, position: Position) -> int:
        """Create a new position and return its ID."""
        async with self.session() as session:
            session.add(position)
            await session.flush()  # Flush to get ID without closing session
            return position.id  # pyright: ignore[reportReturnType]

    async def get_open_positions(self) -> list[Position]:
        """Get all open and partially closed positions that need monitoring."""
        async with self.session() as session:
            result = await session.exec(
                select(Position).where(
                    Position.status.in_(["open", "partially_closed"])  # pyright: ignore[reportAttributeAccessIssue]
                )
            )
            return list(result.all())

    async def get_closed_positions(self, limit: int = 20) -> list[Position]:
        """Get closed positions ordered by close time (most recent first)."""
        async with self.session() as session:
            result = await session.exec(
                select(Position)
                .where(Position.status == "closed")
                .order_by(Position.closed_at.desc())  # pyright: ignore
                .limit(limit)
            )
            return list(result.all())

    async def has_open_position_for_symbol(self, symbol: str) -> bool:
        """Check if any open position exists for symbol."""
        async with self.session() as session:
            result = await session.exec(
                select(Position)
                .where(Position.symbol == symbol, Position.status == "open")
                .limit(1)
            )
            return result.first() is not None

    async def has_open_position_for_symbol_and_exchanges(
        self, symbol: str, exchange1: str, exchange2: str
    ) -> bool:
        """Check if either exchange is used in an open position for this symbol."""
        async with self.session() as session:
            result = await session.exec(
                select(Position)
                .where(
                    Position.symbol == symbol,
                    Position.status == "open",
                    or_(
                        Position.long_exchange == exchange1,
                        Position.long_exchange == exchange2,
                        Position.short_exchange == exchange1,
                        Position.short_exchange == exchange2,
                    ),
                )
                .limit(1)
            )
            return result.first() is not None

    async def get_position(self, position_id: int) -> Optional[Position]:
        """Get position by ID."""
        async with self.session() as session:
            return await session.get(Position, position_id)

    async def close_position(self, position_id: int, exit_data: dict):
        """Close a position with exit data."""
        async with self.session() as session:
            position = await session.get(Position, position_id)
            if not position:
                raise ValueError(f"Position {position_id} not found")

            # Update position fields
            position.status = "closed"
            position.closed_at = datetime.now()
            position.exit_long_price = exit_data["exit_long_price"]
            position.exit_short_price = exit_data["exit_short_price"]
            position.exit_spread_pct = exit_data["exit_spread_pct"]
            position.gross_profit_usd = exit_data["gross_profit_usd"]
            position.exit_fees_usd = exit_data["exit_fees_usd"]
            position.total_fees_usd = exit_data["total_fees_usd"]
            position.net_profit_usd = exit_data["net_profit_usd"]
            position.roi_pct = exit_data["roi_pct"]
            position.close_reason = exit_data["close_reason"]

            session.add(position)
            # Commit happens automatically via context manager

    async def get_position_stats(self) -> dict:
        """Get aggregate position statistics efficiently."""
        async with self.session() as session:
            # Single query for counts
            counts_result = await session.exec(
                select(
                    func.sum(func.iif(Position.status == "open", 1, 0)).label(
                        "open_count"
                    ),
                    func.sum(func.iif(Position.status == "closed", 1, 0)).label(
                        "closed_count"
                    ),
                )
            )
            counts = counts_result.one()
            open_count = int(counts[0] or 0)
            closed_count = int(counts[1] or 0)

            # Efficient aggregation for closed positions
            if closed_count > 0:
                stats_result = await session.exec(
                    select(
                        func.sum(Position.net_profit_usd).label("total_pnl"),
                        func.sum(func.iif(Position.net_profit_usd > 0, 1, 0)).label(  # pyright: ignore[reportOptionalOperand]
                            "wins"
                        ),
                        func.avg(Position.roi_pct).label("avg_roi"),
                    ).where(Position.status == "closed")
                )
                stats = stats_result.one()
                total_pnl = float(stats[0] or 0)
                wins = int(stats[1] or 0)
                avg_roi = float(stats[2] or 0)
                win_rate = (wins / closed_count * 100) if closed_count > 0 else 0
            else:
                total_pnl = 0.0
                win_rate = 0.0
                avg_roi = 0.0

            return {
                "open_positions": open_count,
                "closed_positions": closed_count,
                "total_pnl_usd": total_pnl,
                "win_rate_pct": win_rate,
                "avg_roi_pct": avg_roi,
            }

    async def create_close_attempt(self, close_attempt: CloseAttempt) -> int:
        """Create a new close attempt record and return its ID."""
        async with self.session() as session:
            session.add(close_attempt)
            await session.flush()
            return close_attempt.id  # pyright: ignore[reportReturnType]

    async def get_close_attempts(
        self, position_id: int, limit: int = 10
    ) -> list[CloseAttempt]:
        """Get close attempts for a position (most recent first)."""
        async with self.session() as session:
            result = await session.exec(
                select(CloseAttempt)
                .where(CloseAttempt.position_id == position_id)
                .order_by(CloseAttempt.attempted_at.desc())  # pyright: ignore
                .limit(limit)
            )
            return list(result.all())

    async def update_position(self, position: Position):
        """Update an existing position."""
        async with self.session() as session:
            session.add(position)
            # Commit happens automatically via context manager

    async def close(self):
        """Dispose engine and close all connections."""
        await self.engine.dispose()
