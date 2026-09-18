from collections.abc import Iterable, Mapping
from enum import Enum

from .exceptions import (
    DuplicateTaskError,
    InvalidTransitionError,
    MissingTaskError,
)


# keep str, Enum for the project contract;
class TaskState(str, Enum):  # noqa: UP042
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


LEGAL_TRANSITIONS: Mapping[TaskState, frozenset[TaskState]] = {
    TaskState.PENDING: frozenset({TaskState.READY}),
    # a ready task that gains an unfinished prerequisite returns to pending
    TaskState.READY: frozenset({TaskState.RUNNING, TaskState.PENDING}),
    TaskState.RUNNING: frozenset({TaskState.COMPLETED, TaskState.FAILED}),
    TaskState.COMPLETED: frozenset(),
    TaskState.FAILED: frozenset(),
}


class ExecutionState:
    def __init__(self, task_ids: Iterable[str] = ()) -> None:
        self._states: dict[str, TaskState] = {}
        for task_id in task_ids:
            self.register(task_id)

    def register(self, task_id: str) -> None:
        if task_id in self._states:
            raise DuplicateTaskError(f"task id already tracked: {task_id}")
        self._states[task_id] = TaskState.PENDING

    def state_of(self, task_id: str) -> TaskState:
        if task_id not in self._states:
            raise MissingTaskError(f"untracked task: {task_id}")
        return self._states[task_id]

    def transition(self, task_id: str, to_state: TaskState) -> None:
        current = self.state_of(task_id)
        if to_state not in LEGAL_TRANSITIONS[current]:
            raise InvalidTransitionError(
                f"illegal transition for {task_id}: "
                f"{current.value} -> {to_state.value}"
            )
        self._states[task_id] = to_state

    def ids_in(self, state: TaskState) -> set[str]:
        return {
            task_id
            for task_id, task_state in self._states.items()
            if task_state == state
        }

    def snapshot(self) -> dict[str, TaskState]:
        return self._states.copy()
