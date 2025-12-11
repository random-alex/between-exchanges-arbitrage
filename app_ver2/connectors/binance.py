"""Binance WebSocket connector."""

import asyncio
import json
import websockets

from app_ver2.connectors.models import Ticker
from app_ver2.connectors.base import BaseConnector, ConnectorConfig


class BinanceConnector(BaseConnector):
    """Binance WebSocket connector with automatic reconnection."""

    def __init__(self, config: ConnectorConfig, logger):
        super().__init__(config, logger)
        self.url = "wss://fstream.binance.com/stream"

    @property
    def ping_interval(self) -> float:
        """Return ping interval in seconds."""
        return 60.0

    async def _connect(self) -> None:
        """Connect to Binance WebSocket."""
        streams = [f"{inst.lower()}@bookTicker" for inst in self.config.instruments]
        stream_names = "/".join(streams)
        full_url = f"{self.url}?streams={stream_names}"
        self.ws = await websockets.connect(full_url)
        self.logger.debug(f"Connected to {full_url}")

    async def _subscribe(self) -> None:
        """Subscribe to orderbook streams (already subscribed via URL)."""
        pass

    async def _send_ping(self) -> None:
        """Send WebSocket ping."""
        await self.ws.ping()

    def _handle_message(self, message: str) -> None:
        """Handle incoming Binance message."""
        # Update WebSocket liveness timestamp FIRST (any message)
        self._update_message_timestamp()

        try:
            data = json.loads(message)

            if "data" not in data:
                return

            book_data = data["data"]

            ticker = Ticker(
                ask_price=float(book_data["a"]),
                ask_qnt=float(book_data["A"]),
                bid_price=float(book_data["b"]),
                bid_qnt=float(book_data["B"]),
                instId=book_data["s"],
                ts=int(book_data["T"]),
                exchange="binance",
            )

            # Update data freshness timestamp AFTER successful parse
            self._update_data_timestamp()

            try:
                self.queue.put_nowait(ticker)
            except asyncio.QueueFull:
                self.logger.queue_full()

        except (KeyError, IndexError, ValueError, json.JSONDecodeError) as e:
            self.logger.parse_error(e, message[:100])
