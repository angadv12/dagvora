import asyncio
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol

from .exceptions import (
    CycleError,
    DagvoraError,
    DuplicateTaskError,
    MissingTaskError,
    TaskStartedError,
)
from .graph import TaskGraph
from .models import TaskSpec
from .proposals import (
    AddTaskProposal,
    MutationProposal,
    ProposalHandle,
    ProposalQueue,
    bind_proposals,
)
from .state import ExecutionState, TaskState


class Executor(Protocol):
    async def execute(self, spec: TaskSpec) -> None:
        ...


@dataclass(frozen=True)
class ProposalOutcome:
    proposal: MutationProposal
    applied: bool
    # none when applied; the error class name and message when rejected
    reason: str | None = None


@dataclass(frozen=True)
class RunSummary:
    completed: tuple[str, ...]
    failed: tuple[str, ...]
    blocked: tuple[str, ...]
    states: Mapping[str, TaskState]
    # one outcome per proposal drained during this run, applied or rejected,
    # in submission order
    proposals: tuple[ProposalOutcome, ...] = ()


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
        # set only while run is parked waiting for work to finish
        self._wakeup: asyncio.Future[None] | None = None
        # workers only enqueue here; run drains and applies the proposals
        self._proposals = ProposalQueue(on_submit=self._wake)

    def add_task(
        self, spec: TaskSpec, *, prerequisites: Iterable[str] = ()
    ) -> None:
        prerequisite_ids = tuple(prerequisites)
        # validate every id before any write so a rejection leaves both stores
        # unchanged; no await here, so the task is never seen without its edges
        if spec.id in self._graph.tasks:
            raise DuplicateTaskError(f"task id already exists: {spec.id}")
        for prerequisite_id in prerequisite_ids:
            if prerequisite_id == spec.id:
                raise CycleError(
                    f"self dependency would create a cycle: {prerequisite_id}"
                )
            if prerequisite_id not in self._graph.tasks:
                raise MissingTaskError(f"missing task: {prerequisite_id}")
            self._state.state_of(prerequisite_id)
        self._state.register(spec.id)
        self._graph.add_task(spec)
        # cannot fail: the new task has no dependents and stays pending
        for prerequisite_id in prerequisite_ids:
            self._graph.add_dependency(prerequisite_id, spec.id)
        self._wake()

    def add_dependency(self, prerequisite_id: str, dependent_id: str) -> None:
        # validate against state before the graph mutates; the graph rejects
        # missing ids and cycles atomically
        dependent_state = self._state.state_of(dependent_id)
        prerequisite_state = self._state.state_of(prerequisite_id)
        if dependent_state not in (TaskState.PENDING, TaskState.READY):
            raise TaskStartedError(
                f"dependent already started: {dependent_id} is "
                f"{dependent_state.value}"
            )
        self._graph.add_dependency(prerequisite_id, dependent_id)
        if (
            dependent_state is TaskState.READY
            and prerequisite_state is not TaskState.COMPLETED
        ):
            self._state.transition(dependent_id, TaskState.PENDING)

    def _wake(self) -> None:
        if self._wakeup is not None and not self._wakeup.done():
            self._wakeup.set_result(None)

    async def _execute(self, spec: TaskSpec) -> None:
        # runs inside the worker's own asyncio task, so the binding lives in
        # that task's context copy and never reaches siblings or run
        handle = ProposalHandle(spec.id, self._proposals)
        bind_proposals(handle)
        try:
            await self._executor.execute(spec)
        finally:
            # a settled worker cannot propose, so a task it left running cannot
            # queue work after run ends
            handle.close()

    def _apply_proposals(self, outcomes: list[ProposalOutcome]) -> None:
        for proposal in self._proposals.drain():
            try:
                if isinstance(proposal, AddTaskProposal):
                    # one atomic call, so the task never exists without its edges
                    self.add_task(
                        proposal.task, prerequisites=proposal.prerequisites
                    )
                else:
                    self.add_dependency(
                        proposal.prerequisite_id, proposal.dependent_id
                    )
            except DagvoraError as error:
                # a rejection changes nothing and never reaches the proposer
                outcomes.append(
                    ProposalOutcome(
                        proposal,
                        applied=False,
                        reason=f"{type(error).__name__}: {error}",
                    )
                )
            else:
                outcomes.append(ProposalOutcome(proposal, applied=True))

    async def run(self) -> RunSummary:
        running: dict[asyncio.Task[None], str] = {}
        outcomes: list[ProposalOutcome] = []

        while True:
            # apply queued proposals before any readiness check, so a settled
            # worker's proposals land before its dependents are promoted
            self._apply_proposals(outcomes)

            for task_id in sorted(self._state.ids_in(TaskState.PENDING)):
                if all(
                    self._state.state_of(prerequisite_id) == TaskState.COMPLETED
                    for prerequisite_id in self._graph.dependencies[task_id]
                ):
                    self._state.transition(task_id, TaskState.READY)

            for task_id in sorted(self._state.ids_in(TaskState.READY)):
                self._state.transition(task_id, TaskState.RUNNING)
                task = asyncio.create_task(self._execute(self._graph.tasks[task_id]))
                running[task] = task_id

            # no await since the drain above, so no worker could submit and
            # the queue is empty whenever the run ends
            if not running:
                break

            # a mutation or a submitted proposal resolves the wakeup so new
            # work starts without waiting for a running task to finish
            self._wakeup = asyncio.get_running_loop().create_future()
            done, _ = await asyncio.wait(
                [*running, self._wakeup], return_when=asyncio.FIRST_COMPLETED
            )
            self._wakeup = None
            for task in done:
                if task not in running:
                    continue
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
            proposals=tuple(outcomes),
        )
