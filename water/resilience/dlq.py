"""Dead Letter Queue (DLQ) for capturing failed task executions."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional


@dataclass
class DeadLetter:
    """A record of a failed task execution with full context for replay."""

    task_id: str
    flow_id: str
    execution_id: str
    input_data: dict
    error: str
    error_type: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    attempts: int = 1


class DeadLetterQueue(ABC):
    """Abstract base class for dead letter queue implementations."""

    @abstractmethod
    async def push(self, letter: DeadLetter) -> None:
        """Add a dead letter to the queue."""
        ...

    @abstractmethod
    async def list_letters(self, flow_id: Optional[str] = None) -> List[DeadLetter]:
        """List dead letters, optionally filtered by flow_id."""
        ...

    @abstractmethod
    async def pop(self, index: int = 0) -> Optional[DeadLetter]:
        """Remove and return a dead letter by index."""
        ...

    @abstractmethod
    async def clear(self) -> None:
        """Remove all dead letters from the queue."""
        ...

    @abstractmethod
    async def size(self) -> int:
        """Return the number of dead letters in the queue."""
        ...


class InMemoryDLQ(DeadLetterQueue):
    """In-memory implementation of a dead letter queue."""

    def __init__(self) -> None:
        self._letters: List[DeadLetter] = []

    async def push(self, letter: DeadLetter) -> None:
        self._letters.append(letter)

    async def list_letters(self, flow_id: Optional[str] = None) -> List[DeadLetter]:
        if flow_id is not None:
            return [l for l in self._letters if l.flow_id == flow_id]
        return list(self._letters)

    async def pop(self, index: int = 0) -> Optional[DeadLetter]:
        if index < 0 or index >= len(self._letters):
            return None
        return self._letters.pop(index)

    async def clear(self) -> None:
        self._letters.clear()

    async def size(self) -> int:
        return len(self._letters)
