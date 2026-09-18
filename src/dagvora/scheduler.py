import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from .graph import TaskGraph
from .models import TaskSpec
from .state import ExecutionState, TaskState


class Executor(Protocol):
    async def execute(self, spec: TaskSpec) -> None:
        ...


@dataclass(frozen=True)
class RunSummary:
    completed: tuple[str, ...]
    failed: tuple[str, ...]
    blocked: tuple[str, ...]
    states: Mapping[str, TaskState]


class Scheduler:
    def __init__(
        self,
        graph: TaskGraph,
        executor: Executor,
        state: ExecutionState | None = None,
    ) -> None:
        self._graph = graph
        self._executor = executor
        self._state = (
            state if state is not None else ExecutionState(graph.tasks.keys())
        )

    async def run(self) -> RunSummary:
        running: dict[asyncio.Task[None], str] = {}

        while True:
            for task_id in sorted(self._state.ids_in(TaskState.PENDING)):
                if all(
                    self._state.state_of(prerequisite_id) == TaskState.COMPLETED
                    for prerequisite_id in self._graph.dependencies[task_id]
                ):
                    self._state.transition(task_id, TaskState.READY)

            for task_id in sorted(self._state.ids_in(TaskState.READY)):
                self._state.transition(task_id, TaskState.RUNNING)
                task = asyncio.create_task(
                    self._executor.execute(self._graph.tasks[task_id])
                )
                running[task] = task_id

            if not running:
                break

            done, _ = await asyncio.wait(
                running, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                task_id = running[task]
                next_state = (
                    TaskState.FAILED
                    if task.exception() is not None
                    else TaskState.COMPLETED
                )
                self._state.transition(task_id, next_state)
                running.pop(task)

        completed = tuple(sorted(self._state.ids_in(TaskState.COMPLETED)))
        failed = tuple(sorted(self._state.ids_in(TaskState.FAILED)))
        blocked = tuple(sorted(self._state.ids_in(TaskState.PENDING)))
        return RunSummary(
            completed=completed,
            failed=failed,
            blocked=blocked,
            states=self._state.snapshot(),
        )
