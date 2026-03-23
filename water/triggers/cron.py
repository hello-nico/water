"""Cron trigger for schedule-based flow execution."""

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Callable

from water.triggers.base import Trigger, TriggerEvent


class CronTrigger(Trigger):
    """Trigger that fires on a cron-like schedule.

    Supports standard five-field cron expressions:
    ``minute hour day month weekday``

    Each field may be ``*`` (any), a single integer, or a comma-separated
    list of integers.

    Args:
        flow_name: The ID of the flow to execute.
        schedule: Cron expression string (default ``"* * * * *"`` = every minute).
        input_data: Static input data to pass to the flow on each invocation.
        transform: Optional callable to transform the input data.
        timezone: Optional timezone label (informational; matching uses UTC).

    Example::

        trigger = CronTrigger(
            flow_name="daily-report",
            schedule="0 9 * * 1-5",   # 9 AM weekdays
            input_data={"report_type": "daily"},
        )
    """

    def __init__(
        self,
        flow_name: str,
        schedule: str = "* * * * *",
        input_data: Optional[Dict[str, Any]] = None,
        transform: Optional[Callable] = None,
        timezone: Optional[str] = None,
    ) -> None:
        super().__init__(flow_name, transform)
        self.schedule = schedule
        self.input_data = input_data or {}
        self.timezone = timezone
        self._task: Optional[asyncio.Task] = None
        self._fired_events: List[TriggerEvent] = []

    @staticmethod
    def _parse_field(field: str, min_val: int, max_val: int) -> List[int]:
        """Parse a single cron field into a list of matching integer values.

        Supports:
        - ``*`` (all values in range)
        - single integer (``5``)
        - comma-separated list (``1,3,5``)
        - range with dash (``1-5``)
        - step with slash (``*/5``, ``1-10/2``)

        Args:
            field: The cron field string.
            min_val: Minimum value for the field.
            max_val: Maximum value for the field.

        Returns:
            Sorted list of integers that match.
        """
        if field == "*":
            return list(range(min_val, max_val + 1))

        # Handle step values: */5 or 1-10/2
        if "/" in field:
            base, step_str = field.split("/", 1)
            step = int(step_str)
            if base == "*":
                return list(range(min_val, max_val + 1, step))
            elif "-" in base:
                start, end = base.split("-", 1)
                return list(range(int(start), int(end) + 1, step))
            else:
                return list(range(int(base), max_val + 1, step))

        # Handle ranges: 1-5
        if "-" in field:
            start, end = field.split("-", 1)
            return list(range(int(start), int(end) + 1))

        # Handle comma-separated: 1,3,5
        if "," in field:
            return sorted(int(v) for v in field.split(","))

        # Single value
        return [int(field)]

    def parse_schedule(self) -> Dict[str, List[int]]:
        """Parse the full cron expression into per-field match lists.

        Returns:
            Dictionary with keys ``minute``, ``hour``, ``day``, ``month``,
            ``weekday``, each mapping to a list of matching integers.

        Raises:
            ValueError: If the schedule does not contain exactly five fields.
        """
        parts = self.schedule.strip().split()
        if len(parts) != 5:
            raise ValueError(
                f"Cron schedule must have 5 fields, got {len(parts)}: {self.schedule!r}"
            )

        return {
            "minute": self._parse_field(parts[0], 0, 59),
            "hour": self._parse_field(parts[1], 0, 23),
            "day": self._parse_field(parts[2], 1, 31),
            "month": self._parse_field(parts[3], 1, 12),
            "weekday": self._parse_field(parts[4], 0, 6),
        }

    def should_run(self, now: Optional[datetime] = None) -> bool:
        """Check whether the trigger should fire at the given time.

        Args:
            now: The datetime to check against (defaults to ``datetime.now(timezone.utc)``).

        Returns:
            ``True`` if all cron fields match the given time.
        """
        if now is None:
            now = datetime.now(timezone.utc)

        parsed = self.parse_schedule()
        return (
            now.minute in parsed["minute"]
            and now.hour in parsed["hour"]
            and now.day in parsed["day"]
            and now.month in parsed["month"]
            and now.weekday() in parsed["weekday"]
        )

    async def _run_loop(self, callback: Optional[Callable] = None) -> None:
        """Internal loop that checks the schedule every 30 seconds."""
        while self._active:
            if self.should_run():
                event = self.create_event(self.input_data, schedule=self.schedule)
                self._fired_events.append(event)
                if callback:
                    await callback(event)
            await asyncio.sleep(30)

    async def start(self, callback: Optional[Callable] = None) -> None:
        """Activate the cron trigger.

        Args:
            callback: Optional async callable invoked each time the trigger fires.
        """
        self._active = True
        if callback:
            self._task = asyncio.create_task(self._run_loop(callback))

    async def stop(self) -> None:
        """Deactivate the cron trigger and cancel the background task."""
        self._active = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
