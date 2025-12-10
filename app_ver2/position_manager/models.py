from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field


class Position(SQLModel, table=True):
    __tablename__ = "positions"  # pyright: ignore[reportAssignmentType]

    id: int = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.now, index=True)

    symbol: str = Field(index=True)
    buy_exchange: str
    buy_instrument: str
    sell_exchange: str
    sell_instrument: str

    entry_buy_price: float
    entry_sell_price: float
    entry_spread_pct: float
    quantity: float
    notional_usd: float
    margin_used_usd: float
    entry_fees_usd: float

    entry_buy_min_qty: float
    entry_sell_min_qty: float
    entry_buy_qty_step: float
    entry_sell_qty_step: float

    leverage: float
    capital_allocated: float

    status: str = Field(default="open", index=True)

    closed_at: Optional[datetime] = None
    exit_buy_price: Optional[float] = None
    exit_sell_price: Optional[float] = None
    exit_spread_pct: Optional[float] = None

    gross_profit_usd: Optional[float] = None
    fees_usd: Optional[float] = None
    net_profit_usd: Optional[float] = None
    roi_pct: Optional[float] = None

    open_reason: str = ""
    close_reason: Optional[str] = None
