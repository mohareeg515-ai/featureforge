"""Small retry helper with exponential backoff for transient failures."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

from featureforge.utils.logging import get_logger

T = TypeVar("T")
_log = get_logger("retry")


class TransientError(Exception):
    """A failure that is worth retrying (network blip, throttling, ...)."""


def retry_call[T](
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    base_delay: float = 0.5,
    retry_on: tuple[type[BaseException], ...] = (TransientError, OSError),
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call ``fn`` up to ``attempts`` times, sleeping base_delay * 2**n between tries."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except retry_on as exc:
            if attempt == attempts:
                raise
            delay = base_delay * 2 ** (attempt - 1)
            _log.warning("transient failure, retrying (%s/%s): %s", attempt, attempts, exc)
            sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover
