import functools
import logging
import time
from typing import Callable, TypeVar, Any


def setup_logging() -> None:
    """
    Configure logging for the pipeline.

    This should be called once at the start of the script.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def log_stage(func):
    """
    Decorator that logs when a pipeline stage starts, finishes, or fails.

    Useful for tracking pipeline progress and runtime.
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        logger = logging.getLogger(func.__module__) # so logs show which module the message came from

        logger.info("Entering stage: %s", func.__name__)
        start = time.time()

        try:
            result = func(*args, **kwargs)
            duration = time.time() - start
            logger.info("Completed stage: %s in %.2fs", func.__name__, duration)
            return result

        except Exception:
            duration = time.time() - start
            logger.exception(
                "Failed stage: %s after %.2fs",
                func.__name__,
                duration,
            )
            raise

    return wrapper  # type: ignore[return-value]