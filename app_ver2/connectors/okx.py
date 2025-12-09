"""OKX WebSocket connector."""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Optional
from okx.websocket.WsPublicAsync import WsPublicAsync

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.connectors.models import Ticker
from .base import BaseConnector, ConnectorConfig

logger = logging.getLogger(__name__)


class OKXConnector(BaseConnector):
    """OKX WebSocket connector with automatic reconnection."""

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        self.ws: Optional[WsPublicAsync] = None
        self.url = "wss://wspap.okx.com:8443/ws/v5/public"

    async def _connect(self) -> None:
        """Connect to OKX WebSocket."""
        self.ws = WsPublicAsync(url=self.url)
        await self.ws.start()

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
                logger.warning(f"[{self.config.name}] Disconnect error: {e}")

    def _handle_message(self, message: str) -> None:
        """Handle incoming OKX message (called from separate thread)."""
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
            # Use put_nowait (thread-safe, non-blocking)
            try:
                self.queue.put_nowait(ticker)
            except asyncio.QueueFull:
                logger.warning(f"[{self.config.name}] Queue full, dropping message")

        except (KeyError, IndexError, ValueError, json.JSONDecodeError) as e:
            logger.warning(f"[{self.config.name}] Parse error: {e}")

    async def _message_loop(self) -> None:
        """Keep connection alive (fix: was calling ws.start() repeatedly)."""
        while self._running:
            await asyncio.sleep(1)
