"""Tests for critical edge case fixes in partial closing."""

import pytest
import pytest_asyncio
from app_ver2.position_manager.models import Position
from app_ver2.position_manager.database import PositionDB
from app_ver2.position_manager.manager import PositionManager


@pytest_asyncio.fixture
async def db():
    """Create a test database."""
    import tempfile
    import os

    # Create unique temp DB for each test
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    db = PositionDB(db_path)
    await db.initialize()
    yield db
    await db.close()

    # Clean up temp file
    try:
        os.unlink(db_path)
        os.unlink(f"{db_path}-wal")
        os.unlink(f"{db_path}-shm")
    except FileNotFoundError:
        pass


@pytest.fixture
def sample_position():
    """Create a sample position for testing."""
    return Position(
        symbol="BTCUSDT",
        long_exchange="bybit",
        short_exchange="okx",
        buy_instrument="BTCUSDT",
        sell_instrument="BTC-USDT-SWAP",
        entry_long_price=50000.0,
        entry_short_price=50100.0,
        entry_spread_pct=0.2,
        quantity=0.1,
        notional_usd=5000.0,
        margin_used_usd=500.0,
        entry_fees_usd=5.0,
        entry_buy_min_qty=0.001,
        entry_sell_min_qty=0.001,
        entry_buy_qty_step=0.001,
        entry_sell_qty_step=0.001,
        leverage=10.0,
        capital_allocated=500.0,
        open_reason="test_position",
    )


class TestCriticalFixes:
    """Test critical edge case fixes."""

    @pytest.mark.asyncio
    async def test_partially_closed_blocks_new_position(self, db, sample_position):
        """Test that partially_closed positions block new positions on same exchanges."""
        # Create position and partially close it
        position_id = await db.create_position(sample_position)
        position = await db.get_position(position_id)

        # Mark as partially closed
        position.status = "partially_closed"
        position.remaining_quantity = 0.05
        await db.update_position(position)

        # Check that same exchanges are blocked
        is_blocked = await db.has_open_position_for_symbol_and_exchanges(
            "BTCUSDT", "bybit", "okx"
        )

        assert is_blocked is True, (
            "Partially closed position should block new positions"
        )

        # Check that different exchanges are not blocked
        is_blocked_different = await db.has_open_position_for_symbol_and_exchanges(
            "BTCUSDT", "binance", "deribit"
        )

        assert is_blocked_different is False, (
            "Different exchanges should not be blocked"
        )

    @pytest.mark.asyncio
    async def test_total_fees_includes_entry_fees(self, db, sample_position):
        """Test that total_fees_usd correctly includes entry fees."""
        manager = PositionManager(
            db=db,
            min_roi=0.05,
            stop_loss_pct=-10.0,
            target_convergence_pct=0.1,
            max_hold_hours=24,
            min_spread_cpt=1.5,
        )

        # Create position
        position_id = await db.create_position(sample_position)
        position = await db.get_position(position_id)

        # Partially close 50%
        exit_spread = {
            "exit_long_price": 50050.0,
            "exit_short_price": 50000.0,
            "spread_pct": 0.1,
            "long_fee_pct": 0.05,
            "short_fee_pct": 0.05,
        }

        await manager.close_position_partial(
            position, 0.05, exit_spread, "test_partial"
        )

        # Check total_fees_usd includes entry fees
        assert position.entry_fees_usd == 5.0
        assert position.exit_fees_usd is not None and position.exit_fees_usd > 0
        assert (
            position.total_fees_usd == position.entry_fees_usd + position.exit_fees_usd
        )

        # Partially close remaining 50%
        await manager.close_position_partial(position, 0.05, exit_spread, "test_final")

        # Total fees should still be entry + all accumulated exit fees
        assert (
            position.total_fees_usd == position.entry_fees_usd + position.exit_fees_usd
        )

    @pytest.mark.asyncio
    async def test_multiple_partial_closes_fees_accumulate(self, db, sample_position):
        """Test that fees accumulate correctly across multiple partial closes."""
        manager = PositionManager(
            db=db,
            min_roi=0.05,
            stop_loss_pct=-10.0,
            target_convergence_pct=0.1,
            max_hold_hours=24,
            min_spread_cpt=1.5,
        )

        position_id = await db.create_position(sample_position)
        position = await db.get_position(position_id)

        initial_entry_fees = position.entry_fees_usd

        exit_spread = {
            "exit_long_price": 50050.0,
            "exit_short_price": 50000.0,
            "spread_pct": 0.1,
            "long_fee_pct": 0.05,
            "short_fee_pct": 0.05,
        }

        # First partial close (33%)
        await manager.close_position_partial(position, 0.033, exit_spread, "partial_1")
        exit_fees_1 = position.exit_fees_usd

        # Second partial close (33%)
        await manager.close_position_partial(position, 0.033, exit_spread, "partial_2")
        exit_fees_2 = position.exit_fees_usd

        # Exit fees should accumulate
        assert exit_fees_2 > exit_fees_1

        # Total fees should always be entry + accumulated exit
        assert position.total_fees_usd == initial_entry_fees + exit_fees_2

    @pytest.mark.asyncio
    async def test_open_positions_includes_partially_closed(self, db, sample_position):
        """Test that get_open_positions includes partially_closed positions."""
        # Create first position
        position_id_1 = await db.create_position(sample_position)

        # Create second position with different exchanges
        position_2 = Position(
            symbol="BTCUSDT",
            long_exchange="binance",
            short_exchange="deribit",
            buy_instrument="BTCUSDT",
            sell_instrument="BTC-USDT-SWAP",
            entry_long_price=50000.0,
            entry_short_price=50100.0,
            entry_spread_pct=0.2,
            quantity=0.1,
            notional_usd=5000.0,
            margin_used_usd=500.0,
            entry_fees_usd=5.0,
            entry_buy_min_qty=0.001,
            entry_sell_min_qty=0.001,
            entry_buy_qty_step=0.001,
            entry_sell_qty_step=0.001,
            leverage=10.0,
            capital_allocated=500.0,
            open_reason="test_position_2",
        )
        await db.create_position(position_2)

        # Mark first as partially closed
        position_1 = await db.get_position(position_id_1)
        position_1.status = "partially_closed"
        position_1.remaining_quantity = 0.05
        await db.update_position(position_1)

        # Get open positions
        open_positions = await db.get_open_positions()

        # Should include both open and partially_closed
        assert len(open_positions) == 2
        statuses = {p.status for p in open_positions}
        assert "open" in statuses
        assert "partially_closed" in statuses


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
