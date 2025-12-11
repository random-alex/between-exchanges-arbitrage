"""OKX WebSocket connector."""

import asyncio
import json
from typing import Optional
from okx.websocket.WsPublicAsync import WsPublicAsync

from app_ver2.connectors.models import Ticker
from app_ver2.connectors.base import BaseConnector, ConnectorConfig


class OKXConnector(BaseConnector):
    """OKX WebSocket connector with automatic reconnection."""

    def __init__(self, config: ConnectorConfig, logger):
        super().__init__(config, logger)
        self.ws: Optional[WsPublicAsync] = None
        self.url = "wss://wspap.okx.com:8443/ws/v5/public"

    async def _connect(self) -> None:
        """Connect to OKX WebSocket."""
        # Always create a fresh WebSocket object to avoid stale connection state
        # The OKX library has internal state that doesn't handle reconnection well
        self.ws = WsPublicAsync(url=self.url)
        await self.ws.start()
        # Give the connection a moment to establish
        await asyncio.sleep(0.5)

    async def _subscribe(self) -> None:
        """Subscribe to orderbook streams."""
        if self.ws:
            args = [
                {"channel": "bbo-tbt", "instId": inst_id}
                for inst_id in self.config.instruments
            ]
            await self.ws.subscribe(args, callback=self._handle_message)

    async def _disconnect(self) -> None:
        """Close OKX WebSocket connection."""
        if self.ws:
            try:
                await self.ws.stop()
            except Exception as e:
                self.logger.base_logger.warning(
                    f"[{self.config.name}] Disconnect error: {e}"
                )
            finally:
                self.ws = None

    def _handle_message(self, message: str) -> None:
        """Handle incoming OKX message (called from separate thread)."""
        # Check if connector is being stopped (thread-safe)
        if self._stop_event.is_set():
            return

        # Update timestamp first, before any processing
        self._update_message_timestamp()

        try:
            data = json.loads(message)
            if "data" not in data:
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

    async def _message_loop(self) -> None:
        """Monitor connection health and keep alive."""
        self.logger.info("Message loop started, monitoring connection health...")

        while self._running:
            await asyncio.sleep(5)  # Check every 5 seconds

            try:
                self._check_connection_health()
            except ConnectionError as e:
                self.logger.base_logger.error(f"[{self.config.name}] {e}")
                raise  # Trigger reconnection
