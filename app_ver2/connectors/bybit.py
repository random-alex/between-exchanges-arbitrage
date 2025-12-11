"""Bybit WebSocket connector."""

import asyncio
from typing import Optional
from pybit.unified_trading import WebSocket
from app_ver2.connectors.models import Ticker
from app_ver2.connectors.base import BaseConnector, ConnectorConfig


class BybitConnector(BaseConnector):
    """Bybit WebSocket connector with automatic reconnection."""

    def __init__(self, config: ConnectorConfig, logger):
        super().__init__(config, logger)
        self.ws: Optional[WebSocket] = None

    async def _connect(self) -> None:
        """Connect to Bybit WebSocket."""
        # Always create a fresh WebSocket object to avoid stale connection state
        # The pybit library has internal state that doesn't handle reconnection well
        self.ws = WebSocket(
            testnet=False,
            channel_type="linear",
            ping_interval=20,  # Send ping every 20s
            ping_timeout=10,  # Wait 10s for pong response
        )

    async def _subscribe(self) -> None:
        """Subscribe to orderbook streams."""
        if self.ws:
            self.ws.orderbook_stream(
                depth=1, symbol=self.config.instruments, callback=self._handle_message
            )

    async def _disconnect(self) -> None:
        """Close Bybit WebSocket connection."""
        if self.ws:
            try:
                self.ws.exit()
            except Exception as e:
                self.logger.base_logger.warning(
                    f"[{self.config.name}] Disconnect error: {e}"
                )
            finally:
                self.ws = None

    def _handle_message(self, message: dict) -> None:
        """Handle incoming Bybit message (called from separate thread)."""
        # Check if connector is being stopped (thread-safe)
        if self._stop_event.is_set():
            return

        # Update timestamp first, before any processing
        self._update_message_timestamp()

        try:
            ticker = Ticker(
                ask_price=float(message["data"]["a"][0][0]),
                ask_qnt=float(message["data"]["a"][0][1]),
                bid_price=float(message["data"]["b"][0][0]),
                bid_qnt=float(message["data"]["b"][0][1]),
                instId=message["data"]["s"],
                ts=int(message["ts"]),
                exchange="bybit",
            )

            try:
                self.queue.put_nowait(ticker)
            except asyncio.QueueFull:
                self.logger.queue_full()

        except (KeyError, IndexError, ValueError) as e:
            self.logger.parse_error(e, str(message)[:100])

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
