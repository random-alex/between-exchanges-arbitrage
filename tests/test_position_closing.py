"""Integration tests for Phase 1 position closing improvements."""

import pytest
import pytest_asyncio
from datetime import datetime, timedelta
from app_ver2.position_manager.models import Position
from app_ver2.position_manager.database import PositionDB
from app_ver2.position_manager.manager import PositionManager
from app_ver2.connectors.models import Ticker
from app_ver2.instrument_fetcher import InstrumentSpec
from app_ver2.utils import validate_close_liquidity_level1, estimate_slippage_simple


@pytest_asyncio.fixture
async def db():
    """Create a test database."""
    db = PositionDB("data/test_positions.db")
    await db.initialize()
    yield db
    await db.close()


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
        quantity=0.1,  # 0.1 BTC
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


@pytest.fixture
def instrument_spec():
    """Create sample instrument specs."""
    return InstrumentSpec(
        contract_size=1.0,
        min_order_qnt=0.001,
        qnt_step=0.001,
        settleCoin="USDT",
        baseCoin="BTC",
        fee_pct=0.05,
    )


class TestLiquidityValidation:
    """Test liquidity validation logic."""

    def test_sufficient_liquidity(self, sample_position, instrument_spec):
        """Test validation with sufficient liquidity."""
        long_ticker = Ticker(
            ask_price=50000.0,
            ask_qnt=1.0,  # More than position qty (0.1)
            bid_price=49950.0,
            bid_qnt=1.0,  # More than position qty
            instId="BTCUSDT",
            ts=int(datetime.now().timestamp() * 1000),
            exchange="bybit",
        )

        short_ticker = Ticker(
            ask_price=50050.0,
            ask_qnt=1.0,
            bid_price=50000.0,
            bid_qnt=1.0,
            instId="BTC-USDT-SWAP",
            ts=int(datetime.now().timestamp() * 1000),
            exchange="okx",
        )

        result = validate_close_liquidity_level1(
            sample_position, long_ticker, short_ticker, instrument_spec, instrument_spec
        )

        assert result["can_close_full"] is True
        assert result["closure_strategy"] == "full"
        assert result["liquidity_ratio"] >= 1.0

    def test_partial_liquidity(self, sample_position, instrument_spec):
        """Test validation with partial liquidity (50%)."""
        long_ticker = Ticker(
            ask_price=50000.0,
            ask_qnt=0.05,  # Only 50% of position qty
            bid_price=49950.0,
            bid_qnt=0.05,
            instId="BTCUSDT",
            ts=int(datetime.now().timestamp() * 1000),
            exchange="bybit",
        )

        short_ticker = Ticker(
            ask_price=50050.0,
            ask_qnt=0.05,  # Only 50% of position qty
            bid_price=50000.0,
            bid_qnt=0.05,
            instId="BTC-USDT-SWAP",
            ts=int(datetime.now().timestamp() * 1000),
            exchange="okx",
        )

        result = validate_close_liquidity_level1(
            sample_position, long_ticker, short_ticker, instrument_spec, instrument_spec
        )

        assert result["can_close_full"] is False
        assert result["can_close_partial"] is True
        assert result["closure_strategy"] == "partial"
        assert result["liquidity_ratio"] == 0.5
        assert result["max_closeable_qty"] == 0.05

    def test_insufficient_liquidity(self, sample_position, instrument_spec):
        """Test validation with insufficient liquidity (below exchange minimum)."""
        long_ticker = Ticker(
            ask_price=50000.0,
            ask_qnt=0.0005,  # Below min order size (0.001)
            bid_price=49950.0,
            bid_qnt=0.0005,
            instId="BTCUSDT",
            ts=int(datetime.now().timestamp() * 1000),
            exchange="bybit",
        )

        short_ticker = Ticker(
            ask_price=50050.0,
            ask_qnt=0.0005,  # Below min order size
            bid_price=50000.0,
            bid_qnt=0.0005,
            instId="BTC-USDT-SWAP",
            ts=int(datetime.now().timestamp() * 1000),
            exchange="okx",
        )

        result = validate_close_liquidity_level1(
            sample_position, long_ticker, short_ticker, instrument_spec, instrument_spec
        )

        assert result["can_close_full"] is False
        assert result["can_close_partial"] is False
        assert result["closure_strategy"] == "wait"
        assert result["max_closeable_qty"] == 0.0005

    def test_asymmetric_liquidity(self, sample_position, instrument_spec):
        """Test with liquidity only on one side (partial close possible)."""
        long_ticker = Ticker(
            ask_price=50000.0,
            ask_qnt=1.0,  # Sufficient
            bid_price=49950.0,
            bid_qnt=1.0,
            instId="BTCUSDT",
            ts=int(datetime.now().timestamp() * 1000),
            exchange="bybit",
        )

        short_ticker = Ticker(
            ask_price=50050.0,
            ask_qnt=0.02,  # Partial (20% but above min)
            bid_price=50000.0,
            bid_qnt=0.02,
            instId="BTC-USDT-SWAP",
            ts=int(datetime.now().timestamp() * 1000),
            exchange="okx",
        )

        result = validate_close_liquidity_level1(
            sample_position, long_ticker, short_ticker, instrument_spec, instrument_spec
        )

        assert result["can_close_full"] is False
        assert result["can_close_partial"] is True  # 0.02 >= min_order_qnt (0.001)
        assert result["closure_strategy"] == "partial"
        assert result["long_ratio"] >= 1.0
        assert round(result["short_ratio"], 1) == 0.2
        assert round(result["liquidity_ratio"], 1) == 0.2  # Limited by short side
        assert result["max_closeable_qty"] == 0.02


class TestSlippageEstimation:
    """Test slippage estimation logic."""

    def test_no_slippage_sufficient_liquidity(self):
        """Test slippage when qty <= available."""
        slippage = estimate_slippage_simple(position_qty=0.1, available_qty=1.0)
        assert slippage == 0.0

    def test_slippage_exact_match(self):
        """Test slippage when qty == available."""
        slippage = estimate_slippage_simple(position_qty=1.0, available_qty=1.0)
        assert slippage == 0.0

    def test_slippage_50_percent_overage(self):
        """Test slippage with 50% overage."""
        slippage = estimate_slippage_simple(position_qty=1.5, available_qty=1.0)
        # Overage = 50%, slippage = 50% * 0.01 = 0.5%
        assert slippage == 0.5

    def test_slippage_capped_at_2_percent(self):
        """Test slippage cap at 2%."""
        slippage = estimate_slippage_simple(position_qty=10.0, available_qty=1.0)
        # Overage = 900%, but capped at 2%
        assert slippage == 2.0

    def test_slippage_100_percent_overage(self):
        """Test slippage with 100% overage (2x position size)."""
        slippage = estimate_slippage_simple(position_qty=2.0, available_qty=1.0)
        # Overage = 100%, slippage = 100% * 0.01 = 1.0%
        assert slippage == 1.0


class TestPartialClose:
    """Test partial position closing."""

    @pytest.mark.asyncio
    async def test_partial_close_basic(self, db, sample_position):
        """Test basic partial close functionality."""
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

        # Close 50% of position
        exit_spread = {
            "exit_long_price": 50050.0,
            "exit_short_price": 50000.0,
            "spread_pct": 0.1,
            "long_fee_pct": 0.05,
            "short_fee_pct": 0.05,
        }

        fully_closed = await manager.close_position_partial(
            position, 0.05, exit_spread, "partial_test"
        )

        assert fully_closed is False
        assert position.remaining_quantity == 0.05
        assert position.status == "partially_closed"
        assert position.net_profit_usd is not None

    @pytest.mark.asyncio
    async def test_partial_close_multiple_chunks(self, db, sample_position):
        """Test closing position in multiple chunks."""
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

        exit_spread = {
            "exit_long_price": 50050.0,
            "exit_short_price": 50000.0,
            "spread_pct": 0.1,
            "long_fee_pct": 0.05,
            "short_fee_pct": 0.05,
        }

        # Close 30%
        fully_closed = await manager.close_position_partial(
            position, 0.03, exit_spread, "partial_1"
        )
        assert fully_closed is False
        assert position.remaining_quantity == 0.07

        # Close another 40%
        fully_closed = await manager.close_position_partial(
            position, 0.04, exit_spread, "partial_2"
        )
        assert fully_closed is False
        assert round(position.remaining_quantity, 2) == 0.03

        # Close remaining 30%
        fully_closed = await manager.close_position_partial(
            position, 0.03, exit_spread, "partial_3"
        )
        assert fully_closed is True
        assert position.status == "closed"
        assert round(position.remaining_quantity, 2) == 0


class TestCloseAttemptTracking:
    """Test close attempt audit trail."""

    @pytest.mark.asyncio
    async def test_record_close_attempt(self, db, sample_position):
        """Test recording close attempts."""
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

        # Record failed close attempt
        attempt_id = await manager.record_close_attempt(
            position=position,
            long_bid_qnt=0.05,
            short_ask_qnt=0.05,
            attempted_long_price=50050.0,
            attempted_short_price=50000.0,
            attempted_spread_pct=0.1,
            success=False,
            failure_reason="insufficient_liquidity",
        )

        assert attempt_id is not None

        # Retrieve attempts
        attempts = await db.get_close_attempts(position_id)
        assert len(attempts) == 1
        assert attempts[0].success is False
        assert attempts[0].failure_reason == "insufficient_liquidity"
        assert attempts[0].liquidity_sufficient is False


class TestRetryBackoff:
    """Test retry logic with exponential backoff."""

    def test_first_attempt_no_delay(self, sample_position):
        """Test that first attempt has no delay."""
        from app_ver2.position_manager.monitor import _should_retry_close

        position = sample_position
        position.close_attempts = 0

        should_retry, delay = _should_retry_close(position)
        assert should_retry is True
        assert delay == 0

    def test_exponential_backoff_delays(self, sample_position):
        """Test exponential backoff delays."""
        from app_ver2.position_manager.monitor import _should_retry_close

        position = sample_position

        # Attempt 1: 30s delay
        position.close_attempts = 1
        position.last_close_attempt_at = datetime.now() - timedelta(seconds=10)
        should_retry, delay = _should_retry_close(position)
        assert should_retry is False
        assert delay > 0

        # Attempt 2: 45s delay (30 * 1.5)
        position.close_attempts = 2
        position.last_close_attempt_at = datetime.now() - timedelta(seconds=20)
        should_retry, delay = _should_retry_close(position)
        assert should_retry is False

        # Attempt with sufficient wait time
        position.close_attempts = 1
        position.last_close_attempt_at = datetime.now() - timedelta(seconds=35)
        should_retry, delay = _should_retry_close(position)
        assert should_retry is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
