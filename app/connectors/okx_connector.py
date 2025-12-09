import asyncio
import json
from okx.websocket.WsPublicAsync import WsPublicAsync
from app.connectors.models import Ticker

INSTRUMENT_IDS = [
    "BTC-USDT-251226",
    "BTC-USDT-251212",
    "BTC-USDT-251219",
    "BTC-USDT-260130",
    "BTC-USDT-260327",
    "BTC-USDT-260626",
    "ETH-USDT-251226",
    "BTC-USDT-251212",
    "BTC-USDT-251219",
    "BTC-USDT-260327",
]
BASE_URL: str = "wss://wspap.okx.com:8443/ws/v5/public"
OKX_STREAM = {}


def callbackFunc(message):
    message = json.loads(message)
    if "data" in message.keys():
        ticker = Ticker(
            ask_price=message["data"][0]["asks"][0][0],
            ask_qnt=message["data"][0]["asks"][0][1],
            bid_price=message["data"][0]["bids"][0][0],
            bid_qnt=message["data"][0]["bids"][0][1],
            instId=message["arg"]["instId"],
            ts=message["data"][0]["ts"],
            exchange="okx",
        )
        OKX_STREAM[ticker.normilized_intrument_id] = ticker
    else:
        print(message)


async def run_okx():
    ws = WsPublicAsync(url=BASE_URL)
    await ws.start()

    # TODO: Simplifing the instrument selection, making it manual for now
    args = [{"channel": "bbo-tbt", "instId": inst_id} for inst_id in INSTRUMENT_IDS]
    await ws.subscribe(args, callback=callbackFunc)

    try:
        while True:
            await ws.start()
    except KeyboardInterrupt:
        await ws.stop()
        await ws.unsubscribe(args, callback=callbackFunc)


if __name__ == "__main__":
    asyncio.run(run_okx())
