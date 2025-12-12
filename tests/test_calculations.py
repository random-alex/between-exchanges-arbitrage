"""Unit tests for position_manager calculations module."""

import sys
from pathlib import Path
import importlib.util

# Load calculations module directly without importing __init__.py
calc_path = (
    Path(__file__).parent.parent / "app_ver2" / "position_manager" / "calculations.py"
)
spec = importlib.util.spec_from_file_location("calculations", calc_path)
calculations = importlib.util.module_from_spec(spec)
spec.loader.exec_module(calculations)

calculate_fees = calculations.calculate_fees
calculate_leg_pnl = calculations.calculate_leg_pnl
calculate_position_pnl = calculations.calculate_position_pnl


def test_fees_calculation_equal_prices():
    """Test fee calculation with equal prices and equal fee percentages."""
    fees = calculate_fees(
        quantity=10,
        long_price=100,
        short_price=100,
        long_fee_pct=0.1,
        short_fee_pct=0.1,
    )
    # Long leg: 10 * 100 * 0.1 / 100 = 1.0
    # Short leg: 10 * 100 * 0.1 / 100 = 1.0
    # Total: 2.0
    assert fees == 2.0


def test_fees_different_prices():
    """Test fee calculation when exit prices diverged from entry."""
    fees = calculate_fees(
        quantity=10,
        long_price=105,
        short_price=95,
        long_fee_pct=0.1,
        short_fee_pct=0.1,
    )
    # Long leg: 10 * 105 * 0.1 / 100 = 1.05
    # Short leg: 10 * 95 * 0.1 / 100 = 0.95
    # Total: 2.0
    assert fees == 2.0


def test_fees_different_fee_rates():
    """Test fee calculation with different fee percentages per exchange."""
    fees = calculate_fees(
        quantity=10,
        long_price=100,
        short_price=100,
        long_fee_pct=0.05,  # Bybit 0.05%
        short_fee_pct=0.1,  # OKX 0.1%
    )
    # Long leg: 10 * 100 * 0.05 / 100 = 0.5
    # Short leg: 10 * 100 * 0.1 / 100 = 1.0
    # Total: 1.5
    assert fees == 1.5


def test_long_leg_profit():
    """Test P&L calculation for profitable long leg."""
    pnl = calculate_leg_pnl(10, 100, 105, is_long=True)
    # (105 - 100) * 10 = 50
    assert pnl == 50


def test_long_leg_loss():
    """Test P&L calculation for losing long leg."""
    pnl = calculate_leg_pnl(10, 100, 95, is_long=True)
    # (95 - 100) * 10 = -50
    assert pnl == -50


def test_short_leg_profit():
    """Test P&L calculation for profitable short leg."""
    pnl = calculate_leg_pnl(10, 101, 96, is_long=False)
    # (101 - 96) * 10 = 50
    assert pnl == 50


def test_short_leg_loss():
    """Test P&L calculation for losing short leg."""
    pnl = calculate_leg_pnl(10, 101, 106, is_long=False)
    # (101 - 106) * 10 = -50
    assert pnl == -50


def test_complete_position_pnl():
    """Test complete position P&L calculation."""
    result = calculate_position_pnl(
        quantity=10,
        entry_long_price=100,
        entry_short_price=101,
        exit_long_price=100.5,
        exit_short_price=100.5,
        entry_fees_usd=2.0,
        exit_fees_usd=2.0,
        margin_used_usd=20.0,
    )

    # Long leg: (100.5 - 100) * 10 = 5
    # Short leg: (101 - 100.5) * 10 = 5
    # Gross: 10
    # Net: 10 - 2 - 2 = 6
    # ROI: 6 / 20 * 100 = 30%

    assert result["gross_pnl_usd"] == 10
    assert result["net_pnl_usd"] == 6
    assert result["roi_pct"] == 30
    assert result["total_fees_usd"] == 4.0
    assert result["exit_fees_usd"] == 2.0


def test_complete_position_pnl_negative():
    """Test complete position P&L with loss."""
    result = calculate_position_pnl(
        quantity=10,
        entry_long_price=100,
        entry_short_price=101,
        exit_long_price=99,  # Long leg lost
        exit_short_price=102,  # Short leg lost
        entry_fees_usd=2.0,
        exit_fees_usd=2.0,
        margin_used_usd=20.0,
    )

    # Long leg: (99 - 100) * 10 = -10
    # Short leg: (101 - 102) * 10 = -10
    # Gross: -20
    # Net: -20 - 2 - 2 = -24
    # ROI: -24 / 20 * 100 = -120%

    assert result["gross_pnl_usd"] == -20
    assert result["net_pnl_usd"] == -24
    assert result["roi_pct"] == -120


def test_exit_spread_calculation():
    """Test exit spread percentage calculation."""
    result = calculate_position_pnl(
        quantity=10,
        entry_long_price=100,
        entry_short_price=101,
        exit_long_price=100.5,
        exit_short_price=100,
        entry_fees_usd=1.0,
        exit_fees_usd=1.0,
        margin_used_usd=20.0,
    )

    # Exit spread: (100.5 - 100) / 100 * 100 = 0.5%
    assert result["exit_spread_pct"] == 0.5


if __name__ == "__main__":
    # Run all tests
    import traceback

    tests = [
        test_fees_calculation_equal_prices,
        test_fees_different_prices,
        test_fees_different_fee_rates,
        test_long_leg_profit,
        test_long_leg_loss,
        test_short_leg_profit,
        test_short_leg_loss,
        test_complete_position_pnl,
        test_complete_position_pnl_negative,
        test_exit_spread_calculation,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            print(f"✅ {test.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"❌ {test.__name__}: {e}")
            traceback.print_exc()
            failed += 1
        except Exception as e:
            print(f"💥 {test.__name__}: {e}")
            traceback.print_exc()
            failed += 1

    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
