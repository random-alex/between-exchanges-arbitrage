"""Base WebSocket connector with automatic reconnection."""

import asyncio
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

                await self._connect()
                await self._subscribe()

                self.state = ConnectionState.CONNECTED
                self._retry_count = 0
                self.logger.info("Connected successfully")

                await self._message_loop()

            except Exception as e:
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
        self._running = False
        self.state = ConnectionState.CLOSED
        await self._disconnect()

    def is_connected(self) -> bool:
        """Check if connector is currently connected."""
        return self.state == ConnectionState.CONNECTED
