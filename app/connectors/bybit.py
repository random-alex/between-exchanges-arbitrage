from pybit.unified_trading import WebSocket
from app.connectors.models import Ticker
import asyncio

INSTRUMENT_IDS = [
    "BTCUSDT-12DEC25",
    "BTCUSDT-19DEC25",
    "BTCUSDT-26DEC25",
    "BTCUSDT-30JAN26",
    "BTCUSDT-27MAR26",
    "BTCUSDT-26JUN26",
    "ETHUSDT-12DEC25",
    "ETHUSDT-19DEC25",
    "ETHUSDT-26DEC25",
    "ETHUSDT-27MAR26",
]
BYBIT_STREAM = {}


def handle_message(message):
    ticker = Ticker(
        ask_price=message["data"]["a"][0][0],
        ask_qnt=message["data"]["a"][0][1],
        bid_price=message["data"]["b"][0][0],
        bid_qnt=message["data"]["b"][0][1],
        instId=message["data"]["s"],
        ts=message["ts"],
        exchange="bybit",
    )
    BYBIT_STREAM[ticker.normilized_intrument_id] = ticker


async def run_bybit():
    ws = WebSocket(
        testnet=False,
        channel_type="linear",
    )

    ws.orderbook_stream(depth=1, symbol=INSTRUMENT_IDS, callback=handle_message)

    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(run_bybit())
