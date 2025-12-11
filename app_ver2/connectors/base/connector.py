"""Base WebSocket connector with automatic reconnection."""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any

import websockets
from websockets.protocol import State

from .config import ConnectorConfig
from .state import ConnectionState
from rate_limited_logger import RateLimitedLogger


class BaseConnector(ABC):
    """Abstract base class for exchange WebSocket connectors."""

    def __init__(self, config: ConnectorConfig, logger: RateLimitedLogger):
        self.config = config
        self.state = ConnectionState.DISCONNECTED
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=config.queue_size)
        self._retry_count = 0
        self._running = False
        self.logger = logger
        self._last_message_time = time.time()  # Any message (WebSocket liveness)
        self._last_data_time = time.time()  # Only data messages (data freshness)
        self._connection_timeout = 30  # seconds without messages before reconnect
        self._stop_event = asyncio.Event()  # Thread-safe stop signal
        self.ws: Any = None  # WebSocket connection object

    @property
    @abstractmethod
    def ping_interval(self) -> float:
        """Seconds between ping messages (exchange-specific)."""
        pass

    @abstractmethod
    async def _connect(self) -> None:
        """Establish WebSocket connection."""
        pass

    @abstractmethod
    async def _subscribe(self) -> None:
        """Subscribe to instruments."""
        pass

    @abstractmethod
    async def _send_ping(self) -> None:
        """Send ping to keep connection alive (exchange-specific format)."""
        pass

    @abstractmethod
    def _handle_message(self, message: str) -> None:
        """Handle incoming message (exchange-specific parsing)."""
        pass

    async def _disconnect(self) -> None:
        """Close WebSocket connection."""
        if self.ws:
            try:
                # Check if connection is still open before attempting close
                if self.ws.state != State.CLOSED:
                    await asyncio.wait_for(self.ws.close(), timeout=5.0)
            except asyncio.TimeoutError:
                self.logger.base_logger.warning(
                    f"[{self.config.name}] Disconnect timeout"
                )
            except Exception as e:
                # Silently handle errors for already-closed connections
                self.logger.base_logger.debug(
                    f"[{self.config.name}] Disconnect error: {e}"
                )
            finally:
                self.ws = None

    async def _message_loop(self) -> None:
        """Process incoming messages with heartbeat and health monitoring."""
        last_ping = time.time()
        last_health_check = time.time()

        while self._running:
            # Validate connection exists and is open
            if not self.ws or self.ws.state != State.OPEN:
                raise ConnectionError("WebSocket connection lost")

            try:
                # Periodic health check (every 5 seconds)
                if time.time() - last_health_check > 5.0:
                    self._check_connection_health()
                    last_health_check = time.time()

                # Send ping at exchange-specific interval
                if time.time() - last_ping > self.ping_interval:
                    try:
                        await self._send_ping()
                        last_ping = time.time()
                    except Exception as e:
                        self.logger.base_logger.warning(
                            f"[{self.config.name}] Ping failed: {e}"
                        )
                        self.ws = None
                        raise ConnectionError(f"Ping failed: {e}")

                # Non-blocking receive with shorter timeout
                message = await asyncio.wait_for(self.ws.recv(), timeout=10.0)
                self._handle_message(message)

            except asyncio.TimeoutError:
                # recv() timeout - check health and continue
                self._check_connection_health()
                continue
            except websockets.exceptions.ConnectionClosed as e:
                self.logger.base_logger.warning(
                    f"[{self.config.name}] Connection closed: {e}"
                )
                self.ws = None
                raise
            except Exception as e:
                self.logger.base_logger.error(
                    f"[{self.config.name}] Message loop error: {e}"
                )
                self.ws = None
                raise

    async def connect_with_retry(self) -> None:
        """Connect with automatic retry on failure."""
        self._running = True

        while self._running and self._retry_count < self.config.max_retries:
            try:
                self.logger.debug(f"Connecting (attempt {self._retry_count + 1})")
                self.state = ConnectionState.CONNECTING

                # Ensure clean state before connecting
                await self._disconnect()

                # Add timeout to connection attempt (10 seconds)
                try:
                    await asyncio.wait_for(self._connect(), timeout=10.0)
                except asyncio.TimeoutError:
                    raise ConnectionError("Connection attempt timed out")

                try:
                    # Add timeout to subscription (10 seconds)
                    await asyncio.wait_for(self._subscribe(), timeout=10.0)
                except asyncio.TimeoutError:
                    await self._disconnect()
                    raise ConnectionError("Subscription attempt timed out")
                except Exception as e:
                    # Cleanup connection if subscription fails
                    self.logger.base_logger.error(
                        f"[{self.config.name}] Subscription failed: {e}"
                    )
                    await self._disconnect()
                    raise

                self.state = ConnectionState.CONNECTED
                self._retry_count = 0
                self.logger.info("Connected successfully")

                # Reset timestamps for health monitoring
                self._last_message_time = time.time()
                self._last_data_time = time.time()

                await self._message_loop()

            except Exception as e:
                # Always cleanup on failure
                await self._disconnect()

                self._retry_count += 1
                self.state = ConnectionState.RECONNECTING
                self.logger.connection_error(
                    e, self._retry_count, self.config.max_retries
                )

                if self._retry_count >= self.config.max_retries:
                    self.logger.base_logger.error(
                        f"[{self.config.name}] Max retries reached"
                    )
                    self.state = ConnectionState.CLOSED
                    break

                delay = self.config.calculate_backoff(self._retry_count - 1)
                self.logger.info(f"Reconnecting in {delay}s...")
                await asyncio.sleep(delay)

    async def stop(self) -> None:
        """Stop the connector gracefully."""
        self.logger.info("Stopping...")
        self._stop_event.set()  # Signal stop to all threads/tasks
        self._running = False
        self.state = ConnectionState.CLOSED
        await self._disconnect()

    def is_connected(self) -> bool:
        """Check if connector is currently connected."""
        return self.state == ConnectionState.CONNECTED

    def _update_message_timestamp(self) -> None:
        """Update last message timestamp (call from _handle_message for any message)."""
        self._last_message_time = time.time()

    def _update_data_timestamp(self) -> None:
        """Update last data message timestamp (call after successful ticker parse)."""
        self._last_data_time = time.time()

    def _check_connection_health(self) -> None:
        """Check if connection is still alive and data is fresh."""
        now = time.time()

        # Check WebSocket liveness (any message)
        time_since_last_message = now - self._last_message_time
        if time_since_last_message > self._connection_timeout:
            raise ConnectionError(
                f"No messages received for {time_since_last_message:.0f}s "
                f"(timeout: {self._connection_timeout}s)"
            )

        # Check data freshness (actual ticker data)
        time_since_last_data = now - self._last_data_time
        staleness_limit = (
            self.config.staleness_threshold * 3
        )  # 3x threshold before reconnect
        if time_since_last_data > staleness_limit:
            raise ConnectionError(
                f"Data stream stale: no ticker data for {time_since_last_data:.0f}s "
                f"(limit: {staleness_limit:.0f}s)"
            )
