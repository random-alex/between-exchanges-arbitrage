"""Rate-limited logging to prevent log spam."""

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Tuple
import threading


@dataclass
class LogStats:
    """Statistics for a specific log message."""

    first_seen: float
    last_seen: float
    count: int = 0
    last_logged: float = 0
    sample_messages: list = field(default_factory=list)
    max_samples: int = 3

    def add_occurrence(self, message: str):
        """Record an occurrence of this log message."""
        self.count += 1
        self.last_seen = time.time()

        if len(self.sample_messages) < self.max_samples:
            self.sample_messages.append(message)


class RateLimitedLogger:
    """Logger that rate-limits repetitive messages and provides summaries."""

    def __init__(
        self, connector_name: str, base_logger: logging.Logger, window: float = 10.0
    ):
        self.name = connector_name
        self.base_logger = base_logger
        self.window = window
        self._stats: Dict[Tuple[str, str], LogStats] = defaultdict(
            lambda: LogStats(first_seen=time.time(), last_seen=time.time())
        )
        self._lock = threading.Lock()
        self._last_summary = time.time()
        self._summary_interval = 60.0

        # Connector statistics
        self._parse_errors = 0
        self._queue_drops = 0
        self._connection_errors = 0

    def _should_log(self, key: Tuple[str, str]) -> bool:
        """Check if enough time has passed to log this message again."""
        stats = self._stats[key]
        now = time.time()

        if stats.count == 0:
            return True

        return (now - stats.last_logged) >= self.window

    def _log_with_rate_limit(self, level: int, category: str, message: str):
        """Log a message with rate limiting."""
        key = (level, category)

        with self._lock:
            stats = self._stats[key]  # pyright: ignore[reportArgumentType]
            stats.add_occurrence(message)
            now = time.time()

            if self._should_log(key):  # pyright: ignore[reportArgumentType]
                if stats.count == 1:
                    self.base_logger.log(level, f"[{category}] {message}")
                else:
                    elapsed = now - stats.last_logged
                    rate = stats.count / elapsed if elapsed > 0 else 0
                    self.base_logger.log(
                        level,
                        f"[{category}] {message} "
                        f"(x{stats.count} in last {elapsed:.1f}s, {rate:.1f}/s)",
                    )

                stats.count = 0
                stats.last_logged = now

            if (now - self._last_summary) >= self._summary_interval:
                self._log_summary()
                self._last_summary = now

    def _log_summary(self):
        """Log a summary of all rate-limited messages."""
        with self._lock:
            now = time.time()
            active_stats = [
                (cat, stats)
                for (lvl, cat), stats in self._stats.items()
                if (now - stats.last_seen) < self._summary_interval
            ]

            if not active_stats:
                return

            summary_lines = ["📊 Error Summary (last 60s):"]
            for category, stats in sorted(active_stats, key=lambda x: -x[1].count):
                if stats.count > 0:
                    duration = now - stats.first_seen
                    rate = stats.count / duration if duration > 0 else 0
                    summary_lines.append(
                        f"  • {category}: {stats.count} occurrences ({rate:.1f}/s)"
                    )

                    if stats.sample_messages:
                        summary_lines.append(
                            f"    Sample: {stats.sample_messages[0][:100]}"
                        )

            if len(summary_lines) > 1:
                self.base_logger.info("\n".join(summary_lines))

    def parse_error(self, error: Exception, message_sample: str = ""):
        """Log a parse error with rate limiting."""
        self._parse_errors += 1
        msg = f"{self.name} parse error: {type(error).__name__}: {str(error)}"
        if message_sample:
            msg += f" | Sample: {message_sample[:200]}..."
        self._log_with_rate_limit(logging.WARNING, f"{self.name}_parse", msg)

    def queue_full(self):
        """Log a queue full event with rate limiting."""
        self._queue_drops += 1
        self._log_with_rate_limit(
            logging.WARNING,
            f"{self.name}_queue",
            f"{self.name} queue full, dropping message",
        )

    def connection_error(self, error: Exception, retry: int, max_retries: int):
        """Log a connection error with rate limiting."""
        self._connection_errors += 1
        self._log_with_rate_limit(
            logging.ERROR,
            f"{self.name}_connection",
            f"{self.name} connection failed: {error} (retry {retry}/{max_retries})",
        )

    def stale_data(self, instrument: str, age_seconds: float):
        """Log stale data with rate limiting."""
        self._log_with_rate_limit(
            logging.WARNING,
            f"{self.name}_stale",
            f"{self.name} stale data for {instrument}: {age_seconds:.1f}s old",
        )

    def info(self, message: str):
        """Log info message (not rate limited)."""
        self.base_logger.info(f"[{self.name}] {message}")

    def debug(self, message: str):
        """Log debug message (not rate limited)."""
        self.base_logger.debug(f"[{self.name}] {message}")

    def get_stats(self) -> dict:
        """Get statistics for this connector."""
        return {
            "parse_errors": self._parse_errors,
            "queue_drops": self._queue_drops,
            "connection_errors": self._connection_errors,
        }

    def force_summary(self):
        """Force log a summary now."""
        self._log_summary()
