"""Base WebSocket connector with automatic reconnection."""

import asyncio
import time
from abc import ABC, abstractmethod


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
        self._last_message_time = time.time()
        self._connection_timeout = 30  # seconds without messages before reconnect
        self._stop_event = asyncio.Event()  # Thread-safe stop signal

    @abstractmethod
    async def _connect(self) -> None:
        """Establish WebSocket connection."""
        pass

    @abstractmethod
    async def _disconnect(self) -> None:
        """Close WebSocket connection."""
        pass

    @abstractmethod
    async def _subscribe(self) -> None:
        """Subscribe to instruments."""
        pass

    @abstractmethod
    async def _message_loop(self) -> None:
        """Process incoming messages."""
        pass

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

                # Reset message timestamp for health monitoring
                self._last_message_time = time.time()

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
        """Update last message timestamp (call from _handle_message)."""
        self._last_message_time = time.time()

    def _check_connection_health(self) -> None:
        """Check if connection is still alive based on message flow."""
        time_since_last_message = time.time() - self._last_message_time
        if time_since_last_message > self._connection_timeout:
            raise ConnectionError(
                f"No messages received for {time_since_last_message:.0f}s "
                f"(timeout: {self._connection_timeout}s)"
            )
