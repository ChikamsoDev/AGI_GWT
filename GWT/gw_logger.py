# GWT/gw_logger.py
import logging
from GWT.messages import Message, GWTConfig

logger = logging.getLogger("GWLogger")


class GWLogger:
    """
    Tracks what happens to messages after they reach the spotlight:
    did they ignite (broadcast), get suppressed but retried, or
    get suppressed and permanently dropped.
    """

    def __init__(self, config: GWTConfig = None):
        # Needed so log_suppressed_retry can report "attempts/max_retries"
        # instead of just the current attempt count.
        self.max_retries = config.max_retries if config is not None else None

    def log_broadcast(self, message: Message, effective_priority: float):
        logger.info(
            f"IGNITED | source={message.source} "
            f"effective_priority={effective_priority:.3f} "
            f"attempts={message.attempts}"
        )

    def log_suppressed_retry(self, message: Message, effective_priority: float):
        cap = self.max_retries if self.max_retries is not None else "?"
        logger.info(
            f"SUPPRESSED (will retry) | source={message.source} "
            f"effective_priority={effective_priority:.3f} "
            f"attempts={message.attempts}/{cap}"
        )

    def log_suppressed_final(self, message: Message, effective_priority: float):
        logger.warning(
            f"SUPPRESSED (final, dropped) | source={message.source} "
            f"effective_priority={effective_priority:.3f} "
            f"attempts={message.attempts}"
        )

    def log_expired(self, message: Message):
        logger.debug(f"EXPIRED | source={message.source}")