"""Bybit WebSocket connector."""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional
from pybit.unified_trading import WebSocket

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.connectors.models import Ticker
from .base import BaseConnector, ConnectorConfig

logger = logging.getLogger(__name__)


class BybitConnector(BaseConnector):
    """Bybit WebSocket connector with automatic reconnection."""

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        self.ws: Optional[WebSocket] = None

    async def _connect(self) -> None:
        """Connect to Bybit WebSocket."""
        self.ws = WebSocket(testnet=False, channel_type="linear")

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
                logger.warning(f"[{self.config.name}] Disconnect error: {e}")

    def _handle_message(self, message: dict) -> None:
        """Handle incoming Bybit message (called from separate thread)."""
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
            # Use put_nowait (thread-safe, non-blocking)
            try:
                self.queue.put_nowait(ticker)
            except asyncio.QueueFull:
                logger.warning(f"[{self.config.name}] Queue full, dropping message")

        except (KeyError, IndexError, ValueError) as e:
            logger.warning(f"[{self.config.name}] Parse error: {e}")

    async def _message_loop(self) -> None:
        """Keep connection alive."""
        while self._running:
            await asyncio.sleep(1)
