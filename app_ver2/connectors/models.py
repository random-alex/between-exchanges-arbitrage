from pydantic import BaseModel, computed_field, Field
from typing import Literal, Optional
from datetime import datetime, timezone


class Book(BaseModel):
    price: float
    quantity: float
    num_orders: float
    side: Literal["ask", "bid"]
    instrument_id: str = Field(alias="instId")
    ts: int
    exchange: Literal["okx", "bybit", "binance", "deribit"]
    seq_id: int = Field(alias="seqId")

    @computed_field
    def datetime(self) -> datetime:
        return datetime.fromtimestamp(self.ts / 1000, tz=timezone.utc)


class Ticker(BaseModel):
    # Level 1 orderbook data (Phase 1 uses only these)
    ask_price: float
    ask_qnt: float  # Phase 1: We validate this for liquidity

    bid_price: float
    bid_qnt: float  # Phase 1: We validate this for liquidity

    instrument_id: str = Field(alias="instId")
    ts: int
    exchange: Literal["okx", "bybit", "binance", "deribit", "bitget", "mexc"]

    # Phase 2: Orderbook depth (levels 2-5) - NOT populated in Phase 1
    # These fields prepare for future multi-level slippage calculation
    ask_depth: Optional[list[tuple[float, float]]] = None  # [(price, qty), ...]
    bid_depth: Optional[list[tuple[float, float]]] = None

    # Phase 2: Cumulative quantities across levels - NOT populated in Phase 1
    cumulative_ask_qnt: Optional[float] = None  # Total qty across top N asks
    cumulative_bid_qnt: Optional[float] = None  # Total qty across top N bids

    @computed_field
    def datetime(self) -> datetime:
        return datetime.fromtimestamp(self.ts / 1000, tz=timezone.utc)

    @computed_field
    def normalized_instrument_id(self) -> str:
        if self.exchange == "okx":
            return self.instrument_id.replace("-", "").replace("SWAP", "")
        elif self.exchange == "bybit":
            if len(self.instrument_id.split("-")) > 1:
                normilized = "".join(
                    [
                        self.instrument_id.split("-")[0],
                        convert_date(self.instrument_id.split("-")[-1]),
                    ]
                )
            else:
                normilized = self.instrument_id
            return normilized
        elif self.exchange == "binance":
            # Binance format: BTCUSDT_251227 -> BTCUSDT251227
            return self.instrument_id.replace("_", "")
        elif self.exchange == "deribit":
            # Deribit format: BTC-27DEC24 -> BTC27DEC24
            if "USDC" in self.instrument_id:
                normilized = self.instrument_id.replace("_USDC-PERPETUAL", "USDT")
            else:
                normilized = "USDT".join(
                    [
                        self.instrument_id.split("-")[0],
                        convert_date(self.instrument_id.split("-")[-1]),
                    ]
                )
            return normilized
        else:
            return self.instrument_id.replace("-", "")


def convert_date(s):
    months = {
        "JAN": "01",
        "FEB": "02",
        "MAR": "03",
        "APR": "04",
        "MAY": "05",
        "JUN": "06",
        "JUL": "07",
        "AUG": "08",
        "SEP": "09",
        "OCT": "10",
        "NOV": "11",
        "DEC": "12",
    }
    return s[5:] + months[s[2:5]] + s[:2]
