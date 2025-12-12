from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field


class Position(SQLModel, table=True):
    """Arbitrage position with long/short naming convention.

    A position consists of:
    - Long leg: Buy on one exchange and hold
    - Short leg: Sell on another exchange
    """

    __tablename__ = "positions"  # pyright: ignore[reportAssignmentType]

    id: int = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.now, index=True)

    # Position identification
    symbol: str = Field(index=True)

    # Exchange information
    long_exchange: str  # Exchange where we're long
    short_exchange: str  # Exchange where we're short
    buy_instrument: str  # Instrument ID on long exchange
    sell_instrument: str  # Instrument ID on short exchange

    # Entry prices
    entry_long_price: float  # Price we entered long at
    entry_short_price: float  # Price we entered short at
    entry_spread_pct: float

    # Position details
    quantity: float
    notional_usd: float
    margin_used_usd: float
    leverage: float
    capital_allocated: float

    # Fees
    entry_fees_usd: float  # One-way entry fees
    exit_fees_usd: Optional[float] = None  # One-way exit fees
    total_fees_usd: Optional[float] = None  # Sum of entry + exit

    # Min quantities and steps (cached from instrument specs)
    entry_buy_min_qty: float
    entry_sell_min_qty: float
    entry_buy_qty_step: float
    entry_sell_qty_step: float

    # Status
    status: str = Field(default="open", index=True)

    # Exit data (populated on close)
    closed_at: Optional[datetime] = None
    exit_long_price: Optional[float] = None  # Price we closed long at
    exit_short_price: Optional[float] = None  # Price we closed short at
    exit_spread_pct: Optional[float] = None

    # P&L (populated on close)
    gross_profit_usd: Optional[float] = None
    net_profit_usd: Optional[float] = None
    roi_pct: Optional[float] = None

    # Metadata
    open_reason: str = ""
    close_reason: Optional[str] = None

    # Phase 1: Close attempt tracking
    close_attempts: int = Field(default=0)
    first_close_attempt_at: Optional[datetime] = None
    last_close_attempt_at: Optional[datetime] = None

    # Phase 1: Partial close tracking
    remaining_quantity: Optional[float] = (
        None  # Tracks remaining qty if partially closed
    )
    long_leg_closed: bool = Field(default=False)
    short_leg_closed: bool = Field(default=False)
    long_leg_closed_at: Optional[datetime] = None
    short_leg_closed_at: Optional[datetime] = None

    # Phase 1: Liquidity warnings
    close_liquidity_warnings: int = Field(default=0)
    last_liquidity_warning_at: Optional[datetime] = None

    # Phase 1: Simple slippage estimation (heuristic-based, not multi-level)
    actual_slippage_estimate: Optional[float] = None  # Estimated slippage in USD


class CloseAttempt(SQLModel, table=True):
    """Records each attempt to close a position for debugging and analysis.

    This audit trail helps identify:
    - Liquidity issues over time
    - Retry patterns
    - Why positions fail to close
    - Success rate improvements
    """

    __tablename__ = "close_attempts"  # pyright: ignore[reportAssignmentType]

    id: int = Field(default=None, primary_key=True)
    position_id: int = Field(foreign_key="positions.id", index=True)
    attempted_at: datetime = Field(default_factory=datetime.now, index=True)

    # Liquidity snapshot at attempt time
    long_bid_qnt: float  # Available quantity on long exchange (sell side)
    short_ask_qnt: float  # Available quantity on short exchange (buy side)
    required_qnt: float  # Position quantity that needs to be closed
    liquidity_sufficient: bool  # Whether level 1 liquidity was sufficient

    # Prices at attempt time
    attempted_long_price: float  # Bid price (to sell long leg)
    attempted_short_price: float  # Ask price (to buy short leg)
    attempted_spread_pct: float

    # Result
    success: bool
    failure_reason: Optional[str] = (
        None  # Why close failed (e.g., "insufficient_liquidity")
    )
    partial_close: bool = Field(default=False)
    closed_quantity: Optional[float] = None  # Quantity actually closed (if partial)
