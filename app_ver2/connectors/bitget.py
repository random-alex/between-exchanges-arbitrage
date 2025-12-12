"""Bitget WebSocket connector."""

import json
import websockets

from app_ver2.connectors.models import Ticker
from app_ver2.connectors.base import BaseConnector, ConnectorConfig


class BitgetConnector(BaseConnector):
    """Bitget WebSocket connector with automatic reconnection."""

    def __init__(self, config: ConnectorConfig, logger, data_store: dict):
        super().__init__(config, logger, data_store)
        self.url = "wss://ws.bitget.com/v2/ws/public"

    @property
    def ping_interval(self) -> float:
        """Return ping interval in seconds."""
        return 20.0

    async def _connect(self) -> None:
        """Connect to Bitget WebSocket."""
        self.ws = await websockets.connect(self.url)
        self.logger.debug("Connected to Bitget")

    async def _subscribe(self) -> None:
        """Subscribe to ticker streams."""
        if self.ws:
            args = [
                {"instType": "USDT-FUTURES", "channel": "books1", "instId": inst}
                for inst in self.config.instruments
            ]
            subscribe_msg = {"op": "subscribe", "args": args}
            await self.ws.send(json.dumps(subscribe_msg))
            self.logger.debug(f"Subscribed to {len(args)} instruments")

    async def _send_ping(self) -> None:
        """Send Bitget-specific JSON ping."""
        await self.ws.send("ping")

    async def _handle_message(self, message: str) -> None:
        """Handle incoming Bitget message."""
        # Update WebSocket liveness timestamp FIRST (any message)
        self._update_message_timestamp()
        if message == "pong":
            return
        try:
            data = json.loads(message)

            # Skip pong, subscription confirmation, and other non-data messages
            if "event" in data or "action" not in data:
                return

            # Only process snapshot and update messages
            if data.get("action") not in ["snapshot", "update"]:
                return

            # Bitget sends data as an array
            if "data" not in data or not data["data"]:
                return

            ticker_data = data["data"][0]

            ticker = Ticker(
                ask_price=float(ticker_data["asks"][0][0]),
                ask_qnt=float(ticker_data["asks"][0][1]),
                bid_price=float(ticker_data["bids"][0][0]),
                bid_qnt=float(ticker_data["bids"][0][1]),
                instId=data["arg"]["instId"],
                ts=int(ticker_data["ts"]),
                exchange="bitget",
            )

            # Update data freshness timestamp AFTER successful parse
            self._update_data_timestamp()

            self.data_store[ticker.normalized_instrument_id] = ticker

        except (KeyError, IndexError, ValueError, json.JSONDecodeError) as e:
            await self.logger.parse_error(e, message[:100])
