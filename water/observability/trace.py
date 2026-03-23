__all__ = [
    "TraceSpan",
    "Trace",
    "TraceStore",
    "TraceCollector",
]

"""
Trace Visualization & Debugging.

Captures per-node timing, I/O, and metadata during flow execution
for visual debugging and performance analysis.
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from water.core.types import SerializableMixin


def _parse_iso_utc(iso_str: str) -> datetime:
    """Parse an ISO 8601 string into a timezone-aware UTC datetime.

    Handles both naive strings (assumes UTC) and aware strings with +00:00.
    Works on Python 3.8+ (where fromisoformat cannot parse +00:00).
    """
    # Strip trailing Z and replace with +00:00 for consistency
    cleaned = iso_str.replace("Z", "+00:00")
    # On Python < 3.11, fromisoformat can't parse +00:00; strip it and add tz manually
    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        # Remove the timezone suffix and parse as naive
        if "+" in cleaned:
            dt = datetime.fromisoformat(cleaned.rsplit("+", 1)[0])
        else:
            dt = datetime.fromisoformat(cleaned)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@dataclass
class TraceSpan(SerializableMixin):
    """A single span in a trace, representing one task execution."""
    span_id: str
    task_id: str
    flow_id: str
    execution_id: str
    parent_span_id: Optional[str] = None
    status: str = "running"  # running, completed, failed, retried, skipped
    input_data: Optional[Dict[str, Any]] = None
    output_data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_ms: Optional[float] = None
    retry_attempt: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "span_id": self.span_id,
            "task_id": self.task_id,
            "flow_id": self.flow_id,
            "execution_id": self.execution_id,
            "parent_span_id": self.parent_span_id,
            "status": self.status,
            "input_data": self.input_data,
            "output_data": self.output_data,
            "error": self.error,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_ms": self.duration_ms,
            "retry_attempt": self.retry_attempt,
            "metadata": self.metadata,
        }


@dataclass
class Trace(SerializableMixin):
    """A complete execution trace for a flow run."""
    trace_id: str
    flow_id: str
    execution_id: str
    status: str = "running"
    spans: List[TraceSpan] = field(default_factory=list)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_ms: Optional[float] = None
    input_data: Optional[Dict[str, Any]] = None
    output_data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "trace_id": self.trace_id,
            "flow_id": self.flow_id,
            "execution_id": self.execution_id,
            "status": self.status,
            "spans": [s.to_dict() for s in self.spans],
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_ms": self.duration_ms,
            "input_data": self.input_data,
            "output_data": self.output_data,
            "error": self.error,
            "metadata": self.metadata,
        }


class TraceStore:
    """
    Persists traces for querying and visualization.

    Uses in-memory storage by default. For production, replace with
    a storage-backed implementation.
    """

    def __init__(self, max_traces: int = 1000):
        self._traces: Dict[str, Trace] = {}
        self.max_traces = max_traces

    def save(self, trace: Trace) -> None:
        """Save or update a trace."""
        self._traces[trace.trace_id] = trace
        # Evict oldest if over limit
        if len(self._traces) > self.max_traces:
            oldest_key = next(iter(self._traces))
            del self._traces[oldest_key]

    def get(self, trace_id: str) -> Optional[Trace]:
        """Get a trace by ID."""
        return self._traces.get(trace_id)

    def find_by_execution(self, execution_id: str) -> Optional[Trace]:
        """Find a trace by execution ID."""
        for trace in self._traces.values():
            if trace.execution_id == execution_id:
                return trace
        return None

    def find_by_flow(self, flow_id: str, limit: int = 50) -> List[Trace]:
        """Find traces by flow ID."""
        matches = [t for t in self._traces.values() if t.flow_id == flow_id]
        matches.sort(key=lambda t: t.started_at or "", reverse=True)
        return matches[:limit]

    def list_traces(self, limit: int = 50, status: Optional[str] = None) -> List[Trace]:
        """List recent traces, optionally filtered by status."""
        traces = list(self._traces.values())
        if status:
            traces = [t for t in traces if t.status == status]
        traces.sort(key=lambda t: t.started_at or "", reverse=True)
        return traces[:limit]

    def delete(self, trace_id: str) -> bool:
        """Delete a trace. Returns True if found."""
        return self._traces.pop(trace_id, None) is not None

    def clear(self) -> None:
        """Clear all traces."""
        self._traces.clear()


class TraceCollector:
    """
    Middleware that hooks into flow execution to capture traces.

    Use as a middleware on a flow::

        collector = TraceCollector(store=trace_store)
        flow.use(collector)
    """

    def __init__(self, store: Optional[TraceStore] = None):
        self.store = store or TraceStore()
        self._active_traces: Dict[str, Trace] = {}

    async def before_task(self, task_id: str, data: dict, context: Any) -> dict:
        """Called before each task execution."""
        exec_id = getattr(context, "execution_id", "unknown")
        flow_id = getattr(context, "flow_id", "unknown")

        # Create or get trace for this execution
        trace = self._active_traces.get(exec_id)
        if trace is None:
            trace = Trace(
                trace_id=f"trace_{uuid.uuid4().hex[:12]}",
                flow_id=flow_id,
                execution_id=exec_id,
                started_at=datetime.now(timezone.utc).isoformat(),
                input_data=getattr(context, "initial_input", None),
            )
            self._active_traces[exec_id] = trace
            self.store.save(trace)

        # Create span for this task
        span = TraceSpan(
            span_id=f"span_{uuid.uuid4().hex[:12]}",
            task_id=task_id,
            flow_id=flow_id,
            execution_id=exec_id,
            input_data=data,
            started_at=datetime.now(timezone.utc).isoformat(),
            retry_attempt=getattr(context, "attempt_number", 1),
        )
        trace.spans.append(span)
        self.store.save(trace)

        return data

    async def after_task(self, task_id: str, data: dict, result: dict, context: Any) -> dict:
        """Called after each task execution."""
        exec_id = getattr(context, "execution_id", "unknown")
        trace = self._active_traces.get(exec_id)
        if trace:
            # Find the most recent span for this task
            for span in reversed(trace.spans):
                if span.task_id == task_id and span.status == "running":
                    span.status = "completed"
                    span.output_data = result
                    span.completed_at = datetime.now(timezone.utc).isoformat()
                    if span.started_at:
                        start = _parse_iso_utc(span.started_at)
                        end = _parse_iso_utc(span.completed_at)
                        span.duration_ms = (end - start).total_seconds() * 1000
                    break
            self.store.save(trace)

        return result

    def complete_trace(self, execution_id: str, output: Any = None, error: str = None) -> None:
        """Mark a trace as completed or failed."""
        trace = self._active_traces.pop(execution_id, None)
        if trace:
            trace.completed_at = datetime.now(timezone.utc).isoformat()
            if trace.started_at:
                start = _parse_iso_utc(trace.started_at)
                end = _parse_iso_utc(trace.completed_at)
                trace.duration_ms = (end - start).total_seconds() * 1000
            if error:
                trace.status = "failed"
                trace.error = error
            else:
                trace.status = "completed"
                trace.output_data = output
            self.store.save(trace)

    def get_store(self) -> TraceStore:
        """Return the underlying trace store."""
        return self.store
