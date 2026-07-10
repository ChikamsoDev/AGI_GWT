# GWT/base_module.py
from typing import TYPE_CHECKING
from GWT.messages import Message
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from global_workspace import GlobalWorkspace


class BaseModule(ABC):
    """
    Every module in the system inherits from this.
    Enforces a common interface so GWT can communicate
    with any module without knowing what it does.
    """

    def __init__(self, name: str, gwt: "GlobalWorkspace"):
        self.name = name
        self.gwt = gwt
        # Register this module to receive broadcasts
        gwt.subscribe(self.on_broadcast)

    def submit(self, content, priority: float, semantic_tags: list[str] = None, metadata: dict = None):
        message = Message(
            source=self.name,
            content=content,
            priority=priority,
            semantic_tags=semantic_tags or [],
            metadata=metadata or {},
        )
        self.gwt.submit(message)

    @abstractmethod
    def on_broadcast(self, message: Message):
        """
        Called by GWT whenever something wins the competition.
        Every subclass must implement this — it's how they receive
        what the system is currently focused on.
        """
        pass