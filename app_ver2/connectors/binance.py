"""Binance WebSocket connector."""

import asyncio
import json
import time
from typing import Any
import websockets

from app_ver2.connectors.models import Ticker
from app_ver2.connectors.base import BaseConnector, ConnectorConfig


class BinanceConnector(BaseConnector):
    """Binance WebSocket connector with automatic reconnection."""

    def __init__(self, config: ConnectorConfig, logger):
        super().__init__(config, logger)
        self.ws: Any = None
        self.url = "wss://fstream.binance.com/stream"

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

    async def _disconnect(self) -> None:
        """Close Binance WebSocket connection."""
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                self.logger.base_logger.warning(
                    f"[{self.config.name}] Disconnect error: {e}"
                )
            finally:
                self.ws = None

    async def _message_loop(self) -> None:
        """Process incoming messages with heartbeat."""
        last_ping = time.time()

        while self._running:
            # Validate connection exists
            if not self.ws:
                raise ConnectionError("WebSocket connection lost")

            try:
                # Send ping every 60 seconds to keep connection alive
                if time.time() - last_ping > 60:
                    await self.ws.ping()
                    last_ping = time.time()

                message = await asyncio.wait_for(self.ws.recv(), timeout=30.0)
                self._handle_message(message)
            except asyncio.TimeoutError:
                self.logger.base_logger.warning(f"[{self.config.name}] Message timeout")
                raise
            except websockets.exceptions.ConnectionClosed:
                self.logger.base_logger.warning(
                    f"[{self.config.name}] Connection closed"
                )
                raise
            except Exception as e:
                self.logger.base_logger.error(
                    f"[{self.config.name}] Message loop error: {e}"
                )
                raise

    def _handle_message(self, message: str) -> None:
        """Handle incoming Binance message."""
        # Update timestamp first, before any processing
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

            try:
                self.queue.put_nowait(ticker)
            except asyncio.QueueFull:
                self.logger.queue_full()

        except (KeyError, IndexError, ValueError, json.JSONDecodeError) as e:
            self.logger.parse_error(e, message[:100])
