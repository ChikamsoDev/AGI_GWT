# GWT/global_workspace.py
import dataclasses
import heapq
import logging
import threading
import time
from typing import Any, Callable, List, Optional, Tuple
from GWT.messages import Message, GWTConfig
from GWT.gw_logger import GWLogger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GWT] %(levelname)s: %(message)s"
)
logger = logging.getLogger("GlobalWorkspace")


class GlobalWorkspace:
    def __init__(self, config: Optional[GWTConfig] = None, gw_logger: Optional[GWLogger] = None):
        if config is None:
            config = GWTConfig()

        self.subscribers: List[Callable[[Message], None]] = []
        self.queue: List = []
        self.counter: int = 0
        self.current_broadcast: Optional[Message] = None

        self.config = config
        self.expiry_seconds = config.expiry_seconds
        self.tick_rate = config.tick_rate

        self.gw_logger = gw_logger or GWLogger(config)

        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # --------------------------------------------------------------
        # ALIGNMENT VETO HOOK
        # --------------------------------------------------------------
        # Alignment isn't built yet, so this stays None (no-op) for now.
        # Design decision (locked in): veto happens RIGHT BEFORE broadcast,
        # after a message has cleared ignition_threshold but before any
        # subscriber callbacks fire. This is deliberate:
        #   - Alignment judges real winning CONTENT, not just a priority
        #     float, so it can make an actual decision instead of guessing
        #     from a number.
        #   - It's the cheapest point to add a hard gate — no change to
        #     compete()/decay/heap logic, no change to Message schema.
        #   - It sits OUTSIDE the learned-weight system on purpose. Weights
        #     shape ordinary priority everywhere; this hook is the hard,
        #     non-learned veto that can't be traded off against reward
        #     pressure (e.g. from Metacognition/RSI's strategy selection).
        #
        # List, not a single hook, on purpose: mirrors how real
        # alignment/safety layers tend to work — multiple independent
        # checks, ALL must pass, rather than one monolithic gate that
        # has to reason about everything at once. Doing this as a list
        # now (before anything depends on the singular form) is free;
        # doing it after modules are already calling a single hook would
        # mean touching every call site again.
        #
        # Contract for whatever gets appended here later:
        #   Callable[[Message], Tuple[bool, str]] -> (allow, reason).
        #   allow=True to let this hook pass, False to veto.
        #   reason is always included (even on allow, can be "") so
        #   log_suppressed_final can record WHICH gate blocked a message
        #   and WHY — "the resource gate vetoed this because X" is far
        #   more useful for debugging alignment behavior than "something
        #   vetoed this."
        #   Hooks must be independent — one hook's outcome must never
        #   influence another's, and order must not matter. This keeps
        #   each gate testable in isolation. Must be fast and
        #   non-blocking, same rule as on_broadcast (see BaseModule) —
        #   these run synchronously in the GWT run-loop thread, once per
        #   tick, on the critical path of every broadcast in the system.
        self.veto_hooks: List[Callable[[Message], Tuple[bool, str]]] = []

    # ------------------------------------------------------------------ #
    #  Subscription                                                        #
    # ------------------------------------------------------------------ #

    def subscribe(self, callback: Callable[[Message], None]):
        with self._lock:
            self.subscribers.append(callback)
        logger.info(f"Subscriber registered: {callback.__qualname__}")

    def unsubscribe(self, callback: Callable[[Message], None]):
        with self._lock:
            if callback in self.subscribers:
                self.subscribers.remove(callback)
                logger.info(f"Subscriber removed: {callback.__qualname__}")

    # ------------------------------------------------------------------ #
    #  Submission                                                          #
    # ------------------------------------------------------------------ #

    def submit(self, message: Message):
        # --------------------------------------------------------------
        # PRIORITY CONTRACT ENFORCEMENT
        # --------------------------------------------------------------
        # Every module is assumed to submit priority on a 0.0-1.0 scale
        # (ignition_threshold, decay math, etc. all assume this range).
        # Decision (locked in): REJECT out-of-range priority, don't clamp.
        # Clamping silently hides the bug (a module submitting 5.0 because
        # someone forgot the convention) instead of surfacing it. Fail
        # loud here, at the one place all messages funnel through, rather
        # than mysteriously breaking ignition math three modules away.
        if not (0.0 <= message.priority <= 1.0):
            raise ValueError(
                f"Message priority out of range [0.0, 1.0]: "
                f"source={message.source} priority={message.priority}. "
                f"All modules must submit priority on a 0-1 scale."
            )

        priority = message.priority
        with self._lock:
            heapq.heappush(self.queue, (-priority, self.counter, message))
            self.counter += 1

        logger.debug(
            f"Message queued | source={message.source} "
            f"priority={priority:.2f} | queue_size={len(self.queue)}"
        )

    def _requeue_locked(self, message: Message):
        """
        Push a message back onto the queue with a FRESH counter value.
        Must be called while holding self._lock.
        Reusing the old counter risks a tie with something else in the
        heap, which would force heapq to compare two Message objects
        directly (frozen dataclass, no __lt__) -> TypeError.
        """
        heapq.heappush(self.queue, (-message.priority, self.counter, message))
        self.counter += 1

    # ------------------------------------------------------------------ #
    #  Competition                                                         #
    # ------------------------------------------------------------------ #

    def _purge_expired(self):
        """Called inside the lock."""
        now = time.time()
        fresh = []
        expired = []
        for entry in self.queue:
            if now - entry[2].timestamp <= self.expiry_seconds:
                fresh.append(entry)
            else:
                expired.append(entry[2])

        if expired:
            heapq.heapify(fresh)
            self.queue = fresh
            logger.debug(f"Purged {len(expired)} expired message(s)")
            for msg in expired:
                self.gw_logger.log_expired(msg)

    def compete(self) -> Tuple[Optional[Message], float]:
        """
        Remove expired messages, then return (winner, effective_priority).
        Returns (None, 0.0) if the queue is empty after purging.
        """
        with self._lock:
            self._purge_expired()
            if not self.queue:
                return None, 0.0

            now = time.time()
            decayed = []
            for (_, counter, msg) in self.queue:
                age = now - msg.timestamp
                effective = max(0.0, msg.priority - (self.config.decay_rate * age))
                decayed.append((-effective, counter, msg))

            heapq.heapify(decayed)
            self.queue = decayed
            neg_effective, _, winner = heapq.heappop(self.queue)
            return winner, -neg_effective

    # ------------------------------------------------------------------ #
    #  Broadcast                                                           #
    # ------------------------------------------------------------------ #

    def broadcast(self) -> Optional[Message]:
        """
        Run competition. If the winner clears the ignition threshold,
        run it past the alignment veto hook (if one is set), then
        broadcast it to subscribers. Otherwise, requeue it with an
        incremented attempt count -- unless it's already hit
        max_retries, in which case it's dropped for good.
        """
        winner, effective = self.compete()
        if winner is None:
            return None

        if effective >= self.config.ignition_threshold:
            # ----------------------------------------------------------
            # ALIGNMENT VETO CHECK
            # ----------------------------------------------------------
            # Runs after ignition, before any subscriber sees the message.
            # No-op today (veto_hooks is empty until Alignment is built).
            # ALL hooks must pass -- any single veto blocks the broadcast.
            # Each hook is evaluated independently: one hook's result
            # never affects another's, and evaluation order is not
            # meaningful (don't rely on it). A vetoed message is logged
            # and dropped -- it does NOT get requeued/retried, since a
            # hard veto is a decision that the content itself shouldn't
            # act on, not a "try again later" situation like a
            # low-priority miss below.
            veto_reasons: List[str] = []
            for hook in self.veto_hooks:
                try:
                    allowed, reason = hook(winner)
                except Exception as e:
                    # Fail closed: if a veto hook itself errors, do NOT
                    # let a broken alignment check silently let everything
                    # through. Treat the errored hook as a veto and log
                    # loudly, but keep checking the remaining hooks so we
                    # still surface every real reason, not just the first
                    # crash.
                    logger.error(
                        f"Veto hook {getattr(hook, '__qualname__', hook)} "
                        f"raised an exception -- failing closed "
                        f"(treating as veto) | source={winner.source}: {e}"
                    )
                    allowed, reason = False, f"hook error: {e}"

                if not allowed:
                    veto_reasons.append(reason or "no reason given")

            if veto_reasons:
                logger.warning(
                    f"VETOED | source={winner.source} "
                    f"priority={winner.priority:.2f} effective={effective:.2f} "
                    f"reasons={veto_reasons}"
                )
                self.gw_logger.log_suppressed_final(winner, effective)
                return None

            self.current_broadcast = winner
            logger.info(
                f"Broadcasting | source={winner.source} "
                f"priority={winner.priority:.2f} effective={effective:.2f}"
            )
            self.gw_logger.log_broadcast(winner, effective)

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

        # Didn't ignite -- decide retry vs. permanent drop
        new_attempts = winner.attempts + 1
        if new_attempts >= self.config.max_retries:
            dropped = dataclasses.replace(winner, attempts=new_attempts)
            self.gw_logger.log_suppressed_final(dropped, effective)
            return None

        requeued = dataclasses.replace(winner, attempts=new_attempts)
        with self._lock:
            self._requeue_locked(requeued)
        self.gw_logger.log_suppressed_retry(requeued, effective)
        return None

    # ------------------------------------------------------------------ #
    #  Run Loop                                                            #
    # ------------------------------------------------------------------ #

    def start(self):
        if self._running:
            logger.warning("GWT is already running.")
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="GWT-RunLoop"
        )
        self._thread.start()
        logger.info("GWT started.")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
        logger.info("GWT stopped.")

    def _run_loop(self):
        while self._running:
            self.broadcast()
            time.sleep(self.tick_rate)

    # ------------------------------------------------------------------ #
    #  Introspection                                                       #
    # ------------------------------------------------------------------ #

    def status(self) -> dict:
        with self._lock:
            return {
                "running": self._running,
                "queue_size": len(self.queue),
                "subscriber_count": len(self.subscribers),
                "current_broadcast_source": (
                    self.current_broadcast.source
                    if self.current_broadcast else None
                ),
                "active_veto_hooks": len(self.veto_hooks),
            }