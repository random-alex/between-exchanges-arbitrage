"""Bybit WebSocket connector."""

import asyncio
import json
import time
from typing import Any
import websockets

from app_ver2.connectors.models import Ticker
from app_ver2.connectors.base import BaseConnector, ConnectorConfig


class BybitConnector(BaseConnector):
    """Bybit WebSocket connector with automatic reconnection."""

    def __init__(self, config: ConnectorConfig, logger):
        super().__init__(config, logger)
        self.ws: Any = None
        self.url = "wss://stream.bybit.com/v5/public/linear"

    async def _connect(self) -> None:
        """Connect to Bybit WebSocket."""
        self.ws = await websockets.connect(self.url)
        self.logger.debug("Connected to Bybit")

    async def _subscribe(self) -> None:
        """Subscribe to orderbook streams."""
        if self.ws:
            # For Bybit, subscribe to orderbook.1.{symbol} for level 1 depth
            args = [f"orderbook.1.{inst}" for inst in self.config.instruments]
            subscribe_msg = {"op": "subscribe", "args": args}
            await self.ws.send(json.dumps(subscribe_msg))
            self.logger.debug(f"Subscribed to {len(args)} instruments")

    async def _disconnect(self) -> None:
        """Close Bybit WebSocket connection."""
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
                # Send ping every 20 seconds to keep connection alive (as per Bybit docs)
                if time.time() - last_ping > 20:
                    ping_msg = {"op": "ping"}
                    await self.ws.send(json.dumps(ping_msg))
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
        """Handle incoming Bybit message."""
        # Update timestamp first, before any processing
        self._update_message_timestamp()

        try:
            data = json.loads(message)

            # Skip pong, subscription confirmation, and other non-data messages
            if "op" in data or "topic" not in data:
                return

            # Bybit level 1 orderbook only sends snapshot messages
            if data.get("type") != "snapshot":
                return

            ticker = Ticker(
                ask_price=float(data["data"]["a"][0][0]),
                ask_qnt=float(data["data"]["a"][0][1]),
                bid_price=float(data["data"]["b"][0][0]),
                bid_qnt=float(data["data"]["b"][0][1]),
                instId=data["data"]["s"],
                ts=int(data["ts"]),
                exchange="bybit",
            )

            try:
                self.queue.put_nowait(ticker)
            except asyncio.QueueFull:
                self.logger.queue_full()

        except (KeyError, IndexError, ValueError, json.JSONDecodeError) as e:
            self.logger.parse_error(e, message[:100])
