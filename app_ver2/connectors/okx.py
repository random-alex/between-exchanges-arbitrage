"""OKX WebSocket connector."""

import asyncio
import json
import time
from typing import Any
import websockets

from app_ver2.connectors.models import Ticker
from app_ver2.connectors.base import BaseConnector, ConnectorConfig


class OKXConnector(BaseConnector):
    """OKX WebSocket connector with automatic reconnection."""

    def __init__(self, config: ConnectorConfig, logger):
        super().__init__(config, logger)
        self.ws: Any = None
        self.url = "wss://wspap.okx.com:8443/ws/v5/public"

    async def _connect(self) -> None:
        """Connect to OKX WebSocket."""
        self.ws = await websockets.connect(self.url)
        self.logger.debug("Connected to OKX")

    async def _subscribe(self) -> None:
        """Subscribe to orderbook streams."""
        if self.ws:
            args = [
                {"channel": "bbo-tbt", "instId": inst_id}
                for inst_id in self.config.instruments
            ]
            subscribe_msg = {"op": "subscribe", "args": args}
            await self.ws.send(json.dumps(subscribe_msg))
            self.logger.debug(f"Subscribed to {len(args)} instruments")

    async def _disconnect(self) -> None:
        """Close OKX WebSocket connection."""
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
                # Send ping every 20 seconds to keep connection alive
                if time.time() - last_ping > 20:
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
        """Handle incoming OKX message."""
        # Update timestamp first, before any processing
        self._update_message_timestamp()

        try:
            data = json.loads(message)

            # Skip subscription confirmation and other non-data messages
            if "event" in data or "data" not in data:
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
