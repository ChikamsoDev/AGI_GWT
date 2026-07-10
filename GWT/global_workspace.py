# GWT/global_workspace.py
import heapq
import logging
import threading
import time
from typing import Any, Callable, List, Optional
from GWT.messages import Message, GWTConfig

# Proper logging replaces print statements
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GWT] %(levelname)s: %(message)s"
)
logger = logging.getLogger("GlobalWorkspace")


class GlobalWorkspace:
    def __init__(self, config: Optional[GWTConfig] = None):
        """
        config: GWT configuration settings
        """
        if config is None:
            config = GWTConfig()

        self.subscribers: List[Callable[[Message], None]] = []
        self.queue: List = []
        self.counter: int = 0
        self.current_broadcast: Optional[Message] = None

        self.config = config
        self.expiry_seconds = config.expiry_seconds
        self.tick_rate = config.tick_rate

        # Lock makes queue operations thread-safe
        self._lock = threading.Lock()

        # Controls the run loop
        self._running = False
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------ #
    #  Subscription                                                        #
    # ------------------------------------------------------------------ #

    def subscribe(self, callback: Callable[[Message], None]):
        """Register a module's callback to receive broadcasts."""
        with self._lock:
            self.subscribers.append(callback)
        logger.info(f"Subscriber registered: {callback.__qualname__}")

    def unsubscribe(self, callback: Callable[[Message], None]):
        """Remove a module's callback."""
        with self._lock:
            if callback in self.subscribers:
                self.subscribers.remove(callback)
                logger.info(f"Subscriber removed: {callback.__qualname__}")

    # ------------------------------------------------------------------ #
    #  Submission                                                          #
    # ------------------------------------------------------------------ #

    def submit(self, message: Message):
        """
        Accept a Message object from a module and add it to the queue.
        """
        # Use the message's timestamp (already set by dataclass)
        priority = message.priority

        with self._lock:
            heapq.heappush(self.queue, (-priority, self.counter, message))
            self.counter += 1

        logger.debug(
            f"Message queued | source={message.source} "
            f"priority={priority:.2f} | queue_size={len(self.queue)}"
        )

    # ------------------------------------------------------------------ #
    #  Competition                                                         #
    # ------------------------------------------------------------------ #

    def _purge_expired(self):
        """
        Remove messages that are too old to be useful.
        Called inside the lock, so no separate lock needed here.
        """
        now = time.time()
        fresh = [
            entry for entry in self.queue
            if now - entry[2].timestamp <= self.expiry_seconds
        ]

        expired_count = len(self.queue) - len(fresh)
        if expired_count > 0:
            heapq.heapify(fresh)  # restore heap property after filtering
            self.queue = fresh
            logger.debug(f"Purged {expired_count} expired message(s)")

    def compete(self) -> Optional[Message]:
        """
        Remove expired messages, then return the highest-priority winner.
        """
        with self._lock:
            self._purge_expired()
            if not self.queue:
                return None
            # Recalculate effective priority based on message age
            now = time.time()
            decayed = []
            for (neg_priority, counter, msg) in self.queue:
                age = now - msg.timestamp
                effective = max(0.0, msg.priority - (self.config.decay_rate * age))
                decayed.append((-effective, counter, msg))

            heapq.heapify(decayed)
            self.queue = decayed
            _, _, winner = heapq.heappop(self.queue)
            return winner

    # ------------------------------------------------------------------ #
    #  Broadcast                                                           #
    # ------------------------------------------------------------------ #

    def broadcast(self) -> Optional[Message]:
        """
        Run competition and notify all subscribers with the winner.
        """
        winner = self.compete()
        if winner is None:
            return None

        self.current_broadcast = winner
        logger.info(
            f"Broadcasting | source={winner.source} "
            f"priority={winner.priority:.2f}"
        )

        with self._lock:
            subscribers_snapshot = list(self.subscribers)

        for callback in subscribers_snapshot:
            try:
                callback(winner)
            except Exception as e:
                logger.error(
                    f"Broadcast failed for {callback.__qualname__}: {e}"
                )

        return winner

    # ------------------------------------------------------------------ #
    #  Run Loop                                                            #
    # ------------------------------------------------------------------ #

    def start(self):
        """Start the GWT run loop in a background thread."""
        if self._running:
            logger.warning("GWT is already running.")
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,        # thread dies when main program exits
            name="GWT-RunLoop"
        )
        self._thread.start()
        logger.info("GWT started.")

    def stop(self):
        """Stop the run loop cleanly."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
        logger.info("GWT stopped.")

    def _run_loop(self):
        """Internal loop: broadcast on every tick."""
        while self._running:
            self.broadcast()
            time.sleep(self.tick_rate)

    # ------------------------------------------------------------------ #
    #  Introspection                                                       #
    # ------------------------------------------------------------------ #

    def status(self) -> dict:
        """Return a snapshot of GWT's current state."""
        with self._lock:
            return {
                "running": self._running,
                "queue_size": len(self.queue),
                "subscriber_count": len(self.subscribers),
                "current_broadcast_source": (
                    self.current_broadcast.source
                    if self.current_broadcast else None
                ),
            }