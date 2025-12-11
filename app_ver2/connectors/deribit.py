"""Deribit WebSocket connector."""

import asyncio
import json
import time
from typing import Any
import websockets
from app_ver2.connectors.models import Ticker
from app_ver2.connectors.base import BaseConnector, ConnectorConfig


class DeribitConnector(BaseConnector):
    """Deribit WebSocket connector with automatic reconnection."""

    def __init__(self, config: ConnectorConfig, logger):
        super().__init__(config, logger)
        self.ws: Any = None
        self.url = "wss://www.deribit.com/ws/api/v2"
        self._msg_id = 0

    async def _connect(self) -> None:
        """Connect to Deribit WebSocket."""
        self.ws = await websockets.connect(self.url)
        self.logger.debug("Connected to Deribit")

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
            self.logger.debug(f"Subscribed to {len(channels)} instruments")

    async def _disconnect(self) -> None:
        """Close Deribit WebSocket connection."""
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
        """Handle incoming Deribit message."""
        # Update timestamp first, before any processing
        self._update_message_timestamp()

        try:
            data = json.loads(message)

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
                self.logger.queue_full()

        except (KeyError, IndexError, ValueError, json.JSONDecodeError) as e:
            self.logger.parse_error(e, message[:100])

    def _get_msg_id(self) -> int:
        """Get next message ID for JSON-RPC."""
        self._msg_id += 1
        return self._msg_id
