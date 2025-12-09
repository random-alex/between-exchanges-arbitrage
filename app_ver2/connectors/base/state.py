"""Connection state management."""

from enum import Enum, auto


class ConnectionState(Enum):
    """WebSocket connection states."""

    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    RECONNECTING = auto()
    CLOSED = auto()

    def __str__(self) -> str:
        return self.name
