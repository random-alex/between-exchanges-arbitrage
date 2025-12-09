"""Deribit WebSocket connector."""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any
import websockets

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.connectors.models import Ticker
from .base import BaseConnector, ConnectorConfig

logger = logging.getLogger(__name__)


class DeribitConnector(BaseConnector):
    """Deribit WebSocket connector with automatic reconnection."""

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        self.ws: Any = None
        self.url = "wss://www.deribit.com/ws/api/v2"
        self._msg_id = 0

    async def _connect(self) -> None:
        """Connect to Deribit WebSocket."""
        self.ws = await websockets.connect(self.url)
        logger.debug(f"[{self.config.name}] Connected to Deribit")

    async def _subscribe(self) -> None:
        """Subscribe to orderbook streams."""
        if self.ws:
            channels = [f"quote.{inst}" for inst in self.config.instruments]

            subscribe_msg = {
                "jsonrpc": "2.0",
                "id": self._get_msg_id(),
                "method": "public/subscribe",
                "params": {"channels": channels},
            }

            await self.ws.send(json.dumps(subscribe_msg))
            logger.debug(
                f"[{self.config.name}] Subscribed to {len(channels)} instruments"
            )

    async def _disconnect(self) -> None:
        """Close Deribit WebSocket connection."""
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                logger.warning(f"[{self.config.name}] Disconnect error: {e}")

    async def _message_loop(self) -> None:
        """Process incoming messages."""
        while self._running and self.ws:
            try:
                message = await asyncio.wait_for(self.ws.recv(), timeout=30.0)
                self._handle_message(message)
            except asyncio.TimeoutError:
                logger.warning(
                    f"[{self.config.name}] Message timeout, connection may be dead"
                )
                raise
            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"[{self.config.name}] Connection closed")
                raise
            except Exception as e:
                logger.error(f"[{self.config.name}] Message loop error: {e}")
                raise

    def _handle_message(self, message: str) -> None:
        """Handle incoming Deribit message."""
        try:
            data = json.loads(message)

            # Deribit uses JSON-RPC 2.0 format
            if "params" not in data or "channel" not in data["params"]:
                return

            if not data["params"]["channel"].startswith("quote."):
                return

            quote_data = data["params"]["data"]

            ticker = Ticker(
                ask_price=float(quote_data["best_ask_price"]),
                ask_qnt=float(quote_data["best_ask_amount"]),
                bid_price=float(quote_data["best_bid_price"]),
                bid_qnt=float(quote_data["best_bid_amount"]),
                instId=quote_data["instrument_name"],
                ts=int(quote_data["timestamp"]),
                exchange="deribit",
            )

            try:
                self.queue.put_nowait(ticker)
            except asyncio.QueueFull:
                logger.warning(f"[{self.config.name}] Queue full, dropping message")

        except (KeyError, ValueError, json.JSONDecodeError) as e:
            logger.warning(f"[{self.config.name}] Parse error: {e}")

    def _get_msg_id(self) -> int:
        """Get next message ID for JSON-RPC."""
        self._msg_id += 1
        return self._msg_id
