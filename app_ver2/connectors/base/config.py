"""Configuration management for connectors."""

from dataclasses import dataclass


@dataclass
class ConnectorConfig:
    """Configuration for WebSocket connector."""

    name: str
    instruments: list[str]
    initial_reconnect_delay: float = 3.0
    max_reconnect_delay: float = 60.0
    max_retries: int = 10
    queue_size: int = 1000
    staleness_threshold: float = 5.0  # seconds

    def calculate_backoff(self, attempt: int) -> float:
        """Calculate exponential backoff delay."""
        return min(
            self.initial_reconnect_delay * (2**attempt), self.max_reconnect_delay
        )
