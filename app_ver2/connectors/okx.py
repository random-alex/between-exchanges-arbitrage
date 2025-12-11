"""OKX WebSocket connector."""

import asyncio
import json
import websockets

from app_ver2.connectors.models import Ticker
from app_ver2.connectors.base import BaseConnector, ConnectorConfig


class OKXConnector(BaseConnector):
    """OKX WebSocket connector with automatic reconnection."""

    def __init__(self, config: ConnectorConfig, logger):
        super().__init__(config, logger)
        self.url = "wss://wspap.okx.com:8443/ws/v5/public"

    @property
    def ping_interval(self) -> float:
        """Return ping interval in seconds."""
        return 20.0

    async def _connect(self) -> None:
        """Connect to OKX WebSocket."""
        self.ws = await websockets.connect(self.url)
        self.logger.debug("Connected to OKX")

    async def _subscribe(self) -> None:
        """Subscribe to orderbook streams."""
        if self.ws:
            args = [
                {"channel": "bbo-tbt", "instId": inst_id}
                for inst_id in self.config.instruments
            ]
            subscribe_msg = {"op": "subscribe", "args": args}
            await self.ws.send(json.dumps(subscribe_msg))
            self.logger.debug(f"Subscribed to {len(args)} instruments")

    async def _send_ping(self) -> None:
        """Send WebSocket ping."""
        await self.ws.ping()

    def _handle_message(self, message: str) -> None:
        """Handle incoming OKX message."""
        # Update timestamp first, before any processing
        self._update_message_timestamp()

        try:
            data = json.loads(message)

            # Skip subscription confirmation and other non-data messages
            if "event" in data or "data" not in data:
                return

            ticker = Ticker(
                ask_price=float(data["data"][0]["asks"][0][0]),
                ask_qnt=float(data["data"][0]["asks"][0][1]),
                bid_price=float(data["data"][0]["bids"][0][0]),
                bid_qnt=float(data["data"][0]["bids"][0][1]),
                instId=data["arg"]["instId"],
                ts=int(data["data"][0]["ts"]),
                exchange="okx",
            )

            try:
                self.queue.put_nowait(ticker)
            except asyncio.QueueFull:
                self.logger.queue_full()

        except (KeyError, IndexError, ValueError, json.JSONDecodeError) as e:
            self.logger.parse_error(e, message[:100])
