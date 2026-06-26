"""Resilience infrastructure for Tableau Metadata API requests.

This module provides:
- AdaptivePageSizeCircuitBreaker: Dynamically adjusts fetch_size per connection type
  based on NODE_LIMIT_EXCEEDED signals from the Tableau GraphQL API.
- ConcurrencyCircuitBreaker: Thread-safe concurrency limiter that reduces parallelism
  on HTTP 429/5xx and recovers after sustained success.

These are generic patterns independent of Tableau business logic.
"""

import logging
import threading
from collections import defaultdict
from typing import Any, Dict

logger = logging.getLogger(__name__)


class AdaptivePageSizeCircuitBreaker:
    """Dynamically adjusts fetch_size per connection type based on NODE_LIMIT errors.

    States:
    - CLOSED: Uses configured page size.
    - OPEN: Halved after failure. Recovers after consecutive successes.
    - HALF_OPEN: Probing higher page sizes incrementally.
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(self, min_page_size: int = 1) -> None:
        self._min_page_size = min_page_size
        self._state: Dict[str, str] = {}
        self._current_page_size: Dict[str, int] = {}
        self._configured_page_size: Dict[str, int] = {}
        self._consecutive_successes: Dict[str, int] = {}
        self._total_trips: Dict[str, int] = defaultdict(int)
        self._total_probes: Dict[str, int] = defaultdict(int)
        self._recovery_threshold = 5
        self._half_open_threshold = 3

    def get_page_size(self, connection_type: str, configured_page_size: int) -> int:
        if connection_type not in self._state:
            self._init_connection(connection_type, configured_page_size)
        return self._current_page_size[connection_type]

    def on_success(self, connection_type: str) -> None:
        if connection_type not in self._state:
            return

        state = self._state[connection_type]
        self._consecutive_successes[connection_type] += 1
        consecutive = self._consecutive_successes[connection_type]

        if state == self.OPEN:
            if consecutive >= self._recovery_threshold:
                self._state[connection_type] = self.HALF_OPEN
                self._consecutive_successes[connection_type] = 0
                probe_size = min(
                    self._current_page_size[connection_type] * 2,
                    self._configured_page_size[connection_type],
                )
                self._current_page_size[connection_type] = probe_size
                self._total_probes[connection_type] += 1
                logger.info(
                    f"[CircuitBreaker] {connection_type}: OPEN -> HALF_OPEN, "
                    f"probing page_size={probe_size}"
                )

        elif state == self.HALF_OPEN:
            if consecutive >= self._half_open_threshold:
                current = self._current_page_size[connection_type]
                configured = self._configured_page_size[connection_type]
                if current >= configured:
                    self._state[connection_type] = self.CLOSED
                    self._consecutive_successes[connection_type] = 0
                    logger.info(
                        f"[CircuitBreaker] {connection_type}: HALF_OPEN -> CLOSED, "
                        f"fully recovered to page_size={configured}"
                    )
                else:
                    probe_size = min(current * 2, configured)
                    self._current_page_size[connection_type] = probe_size
                    self._consecutive_successes[connection_type] = 0
                    self._total_probes[connection_type] += 1
                    logger.info(
                        f"[CircuitBreaker] {connection_type}: "
                        f"HALF_OPEN probing higher, page_size={probe_size}"
                    )

    def on_node_limit(self, connection_type: str) -> int:
        if connection_type not in self._state:
            return self._min_page_size

        previous = self._current_page_size[connection_type]
        reduced = max(previous // 2, self._min_page_size)
        self._current_page_size[connection_type] = reduced
        self._consecutive_successes[connection_type] = 0
        self._total_trips[connection_type] += 1

        old_state = self._state[connection_type]
        self._state[connection_type] = self.OPEN

        logger.info(
            f"[CircuitBreaker] {connection_type}: {old_state} -> OPEN, "
            f"page_size {previous} -> {reduced} "
            f"(trip #{self._total_trips[connection_type]})"
        )
        return reduced

    def get_stats(self) -> Dict[str, Dict[str, Any]]:
        stats: Dict[str, Dict[str, Any]] = {}
        for conn_type in self._state:
            stats[conn_type] = {
                "state": self._state[conn_type],
                "current_page_size": self._current_page_size[conn_type],
                "configured_page_size": self._configured_page_size[conn_type],
                "total_trips": self._total_trips[conn_type],
                "total_probes": self._total_probes[conn_type],
            }
        return stats

    def _init_connection(self, connection_type: str, configured_page_size: int) -> None:
        self._state[connection_type] = self.CLOSED
        self._current_page_size[connection_type] = configured_page_size
        self._configured_page_size[connection_type] = configured_page_size
        self._consecutive_successes[connection_type] = 0


class ConcurrencyCircuitBreaker:
    """Thread-safe concurrency limiter with circuit breaker recovery.

    States:
    - CLOSED: Running at max concurrency.
    - OPEN: Reduced after throttle/error signals. Recovers after sustained success.
    - HALF_OPEN: Probing higher concurrency (+1 worker at a time).
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(
        self,
        max_workers: int = 4,
        min_workers: int = 1,
        recovery_threshold: int = 10,
        half_open_threshold: int = 5,
    ) -> None:
        self._max_workers = max_workers
        self._min_workers = min_workers
        self._current_workers = max_workers
        self._state = self.CLOSED
        self._consecutive_successes = 0
        self._recovery_threshold = recovery_threshold
        self._half_open_threshold = half_open_threshold
        self._total_trips = 0
        self._total_probes = 0
        self._total_throttles = 0
        self._lock = threading.Lock()

    @property
    def current_workers(self) -> int:
        return self._current_workers

    def on_success(self) -> None:
        with self._lock:
            self._consecutive_successes += 1

            if self._state == self.OPEN:
                if self._consecutive_successes >= self._recovery_threshold:
                    self._state = self.HALF_OPEN
                    self._consecutive_successes = 0
                    probe = min(self._current_workers + 1, self._max_workers)
                    self._current_workers = probe
                    self._total_probes += 1
                    logger.info(
                        f"[ConcurrencyCB] OPEN -> HALF_OPEN, probing workers={probe}"
                    )

            elif self._state == self.HALF_OPEN:
                if self._consecutive_successes >= self._half_open_threshold:
                    if self._current_workers >= self._max_workers:
                        self._state = self.CLOSED
                        self._consecutive_successes = 0
                        logger.info(
                            f"[ConcurrencyCB] HALF_OPEN -> CLOSED, "
                            f"recovered to workers={self._max_workers}"
                        )
                    else:
                        probe = min(self._current_workers + 1, self._max_workers)
                        self._current_workers = probe
                        self._consecutive_successes = 0
                        self._total_probes += 1
                        logger.info(
                            f"[ConcurrencyCB] HALF_OPEN probing workers={probe}"
                        )

    def on_throttle(self) -> None:
        with self._lock:
            self._total_throttles += 1
            previous = self._current_workers
            reduced = max(self._current_workers // 2, self._min_workers)
            if reduced == previous and reduced > self._min_workers:
                reduced = self._min_workers
            self._current_workers = reduced
            self._consecutive_successes = 0
            self._total_trips += 1

            old_state = self._state
            self._state = self.OPEN
            logger.info(
                f"[ConcurrencyCB] {old_state} -> OPEN, "
                f"workers {previous} -> {reduced} "
                f"(throttle #{self._total_throttles})"
            )

    def get_stats(self) -> Dict[str, Any]:
        return {
            "state": self._state,
            "current_workers": self._current_workers,
            "max_workers": self._max_workers,
            "total_trips": self._total_trips,
            "total_probes": self._total_probes,
            "total_throttles": self._total_throttles,
        }
