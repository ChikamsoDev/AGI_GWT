# GWT/base_module.py
import logging
import queue
import threading
from typing import TYPE_CHECKING, Optional
from GWT.messages import Message
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from global_workspace import GlobalWorkspace

logger = logging.getLogger("BaseModule")


class BaseModule(ABC):
    """
    Every module in the system inherits from this.
    Enforces a common interface so GWT can communicate
    with any module without knowing what it does.

    --------------------------------------------------------------------
    NON-BLOCKING BROADCAST CONTRACT (locked in)
    --------------------------------------------------------------------
    GWT calls on_broadcast() SYNCHRONOUSLY, on its own run-loop thread,
    for every subscriber, once per tick. If any one module's handling of
    a broadcast blocks (disk I/O, a lock, a slow LLM tool call), it stalls
    ignition for every other module in the system for that entire duration.

    To make this a structural guarantee instead of a rule every module
    author has to remember, on_broadcast() is now FINAL (not abstract).
    It does exactly one thing: drop the message into this module's own
    inbox and return immediately. Subclasses do NOT override
    on_broadcast() -- they override _process(message) instead, which
    runs on the module's own dedicated worker thread, on its own time,
    completely decoupled from GWT's tick rate.

    This is exactly the pattern that will let Working Memory hold "what
    was broadcast two cycles ago while I finish integrating it" instead
    of chasing a value that's already moved on -- each module now has
    its own timeline once a message reaches it.
    """

    def __init__(self, name: str, gwt: "GlobalWorkspace", inbox_maxsize: int = 0):
        self.name = name
        self.gwt = gwt

        # --------------------------------------------------------------
        # Inbox + worker thread. inbox_maxsize=0 means unbounded (default
        # Queue behavior) -- pass a positive number if a module wants
        # backpressure instead of unbounded growth under sustained load.
        self._inbox: "queue.Queue[Message]" = queue.Queue(maxsize=inbox_maxsize)
        self._worker_thread: Optional[threading.Thread] = None
        self._running = False

        # Register this module to receive broadcasts
        gwt.subscribe(self.on_broadcast)

    # ------------------------------------------------------------------ #
    #  Outbound (module -> GWT)                                           #
    # ------------------------------------------------------------------ #

    def submit(self, content, priority: float, semantic_tags: list[str] = None, metadata: dict = None):
        message = Message(
            source=self.name,
            content=content,
            priority=priority,
            semantic_tags=semantic_tags or [],
            metadata=metadata or {},
        )
        self.gwt.submit(message)

    # ------------------------------------------------------------------ #
    #  Inbound (GWT -> module)                                            #
    # ------------------------------------------------------------------ #

    def on_broadcast(self, message: Message):
        """
        FINAL. Do not override this in subclasses.

        Called by GWT on its own run-loop thread whenever something wins
        the competition. Must return immediately -- all it does is hand
        the message to this module's own inbox. Real handling happens in
        _process(), on this module's own worker thread.
        """
        try:
            self._inbox.put_nowait(message)
        except queue.Full:
            # Only reachable if inbox_maxsize was set > 0. A full inbox
            # means this module is falling behind its own broadcast rate.
            # Drop-and-log rather than block -- blocking here would
            # reintroduce the exact stall this pattern exists to prevent.
            logger.warning(
                f"{self.name}: inbox full, dropping broadcast from "
                f"source={message.source}"
            )

    @abstractmethod
    def _process(self, message: Message):
        """
        Every subclass must implement this. This is where the actual
        work of handling a broadcast happens -- it runs on this module's
        own worker thread, not GWT's run-loop thread, so it is free to
        block, do I/O, call an LLM tool, etc. without affecting anyone
        else's ability to receive broadcasts.
        """
        pass

    def _worker_loop(self):
        while self._running:
            try:
                # Timeout lets the loop periodically re-check self._running
                # so stop() doesn't have to wait indefinitely on a get().
                message = self._inbox.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                self._process(message)
            except Exception as e:
                logger.error(f"{self.name}: error processing message: {e}")

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def start(self):
        if self._running:
            logger.warning(f"{self.name} worker already running.")
            return
        self._running = True
        self._worker_thread = threading.Thread(
            target=self._worker_loop, daemon=True, name=f"{self.name}-Worker"
        )
        self._worker_thread.start()
        logger.info(f"{self.name} worker started.")

    def stop(self):
        # Mirrors the gwt.subscribe() call in __init__. Without this, GWT
        # keeps calling on_broadcast() on a module whose worker thread has
        # already died -- on_broadcast only does inbox.put_nowait(), so
        # with nothing left to drain it, the inbox grows unbounded forever.
        # This is routine lifecycle symmetry, NOT a self-modification
        # action -- it only ever removes THIS module's own subscription,
        # only during its own normal shutdown, never another module's.
        # Forced unsubscribe of another (e.g. malfunctioning) module is a
        # different, harder question that belongs to Metacognition/RSI +
        # Alignment once those exist -- deliberately not addressed here.
        self.gwt.unsubscribe(self.on_broadcast)

        self._running = False
        if self._worker_thread:
            self._worker_thread.join(timeout=2.0)
        logger.info(f"{self.name} worker stopped.")