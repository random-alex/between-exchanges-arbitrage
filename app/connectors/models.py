from pydantic import BaseModel, computed_field, Field
from typing import Literal
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
    ask_price: float
    ask_qnt: float

    bid_price: float
    bid_qnt: float

    instrument_id: str = Field(alias="instId")
    ts: int
    exchange: Literal["okx", "bybit", "binance", "deribit"]

    @computed_field
    def datetime(self) -> datetime:
        return datetime.fromtimestamp(self.ts / 1000, tz=timezone.utc)

    @computed_field
    def normilized_intrument_id(self) -> str:
        if self.exchange == "okx":
            return self.instrument_id.replace("-", "")
        elif self.exchange == "bybit":
            normilized = "".join(
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
