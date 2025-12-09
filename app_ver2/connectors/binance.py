"""Binance WebSocket connector."""

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


class BinanceConnector(BaseConnector):
    """Binance WebSocket connector with automatic reconnection."""

    def __init__(self, config: ConnectorConfig):
        super().__init__(config)
        self.ws: Any = None  # websockets connection
        self.url = "wss://fstream.binance.com/stream"

    async def _connect(self) -> None:
        """Connect to Binance WebSocket."""
        # Build stream names for combined stream
        streams = [f"{inst.lower()}@bookTicker" for inst in self.config.instruments]
        stream_names = "/".join(streams)
        full_url = f"{self.url}?streams={stream_names}"

        self.ws = await websockets.connect(full_url)
        logger.debug(f"[{self.config.name}] Connected to {full_url}")

    async def _subscribe(self) -> None:
        """Subscribe to orderbook streams (already subscribed via URL)."""
        # Binance combined stream subscribes via URL, no separate subscribe needed
        pass

    async def _disconnect(self) -> None:
        """Close Binance WebSocket connection."""
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
                raise  # Trigger reconnection
            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"[{self.config.name}] Connection closed")
                raise  # Trigger reconnection
            except Exception as e:
                logger.error(f"[{self.config.name}] Message loop error: {e}")
                raise

    def _handle_message(self, message: str) -> None:
        """Handle incoming Binance message."""
        try:
            data = json.loads(message)

            # Binance combined stream format: {"stream": "...", "data": {...}}
            if "data" not in data:
                return

            book_data = data["data"]

            ticker = Ticker(
                ask_price=float(book_data["a"]),  # Best ask price
                ask_qnt=float(book_data["A"]),  # Best ask quantity
                bid_price=float(book_data["b"]),  # Best bid price
                bid_qnt=float(book_data["B"]),  # Best bid quantity
                instId=book_data["s"],  # Symbol
                ts=int(book_data["T"]),  # Transaction time
                exchange="binance",
            )

            # Use put_nowait (thread-safe, non-blocking)
            try:
                self.queue.put_nowait(ticker)
            except asyncio.QueueFull:
                logger.warning(f"[{self.config.name}] Queue full, dropping message")

        except (KeyError, ValueError, json.JSONDecodeError) as e:
            logger.warning(f"[{self.config.name}] Parse error: {e}")
