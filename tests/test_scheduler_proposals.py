import asyncio
from collections.abc import Awaitable, Callable

import pytest

from dagvora.exceptions import (
    CycleError,
    DuplicateTaskError,
    MissingTaskError,
    ProposalContextError,
    TaskStartedError,
)
from dagvora.graph import TaskGraph
from dagvora.models import TaskSpec
from dagvora.proposals import (
    AddDependencyProposal,
    AddTaskProposal,
    MutationProposal,
    ProposalHandle,
    current_proposals,
)
from dagvora.scheduler import ProposalOutcome, Scheduler
from dagvora.state import ExecutionState, TaskState

Script = Callable[[], Awaitable[None]]


class _ScriptedExecutor:
    # runs an optional async script per task id and records start and finish
    def __init__(self) -> None:
        self.scripts: dict[str, Script] = {}
        self.events: list[tuple[str, str]] = []
        self._started: dict[str, asyncio.Event] = {}

    def started(self, task_id: str) -> asyncio.Event:
        # created on first use, so asking after the start still sees it set
        return self._started.setdefault(task_id, asyncio.Event())

    @property
    def start_order(self) -> list[str]:
        return [task_id for kind, task_id in self.events if kind == "start"]

    def before(self, first: tuple[str, str], second: tuple[str, str]) -> bool:
        return self.events.index(first) < self.events.index(second)

    async def execute(self, spec: TaskSpec) -> None:
        self.events.append(("start", spec.id))
        self.started(spec.id).set()
        try:
            script = self.scripts.get(spec.id)
            if script is not None:
                await script()
        finally:
            self.events.append(("finish", spec.id))


def _spec(task_id: str, title: str | None = None) -> TaskSpec:
    return TaskSpec(id=task_id, title=title or task_id)


def _add_tasks(graph: TaskGraph, *task_ids: str) -> None:
    for task_id in task_ids:
        graph.add_task(_spec(task_id))


def _topology(
    graph: TaskGraph,
) -> tuple[dict[str, TaskSpec], dict[str, set[str]], dict[str, set[str]]]:
    return (
        dict(graph.tasks),
        {task_id: set(ids) for task_id, ids in graph.dependencies.items()},
        {task_id: set(ids) for task_id, ids in graph.dependents.items()},
    )


def test_worker_proposes_a_task_that_then_runs() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A")
        state = ExecutionState(graph.tasks.keys())
        executor = _ScriptedExecutor()
        seen: dict[str, object] = {}

        async def worker_a() -> None:
            proposal = current_proposals().propose_task(_spec("X"))
            # the worker only queued the proposal; the scheduler applies it
            seen["proposal"] = proposal
            seen["x_in_graph"] = "X" in graph.tasks
            seen["x_in_state"] = "X" in state.snapshot()
            # A cannot finish until X starts, so the submit must wake the loop
            await executor.started("X").wait()

        executor.scripts["A"] = worker_a
        summary = await Scheduler(graph, executor, state).run()

        assert seen["x_in_graph"] is False
        assert seen["x_in_state"] is False
        assert executor.before(("start", "X"), ("finish", "A"))
        assert summary.completed == ("A", "X")
        assert summary.proposals == (
            ProposalOutcome(seen["proposal"], applied=True),
        )
        assert summary.proposals[0].proposal.proposed_by == "A"
        # the binding stayed inside the worker's task
        with pytest.raises(ProposalContextError):
            current_proposals()

    asyncio.run(scenario())


def test_worker_proposes_a_prerequisite_that_a_pending_dependent_waits_for() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B")
        graph.add_dependency("A", "B")
        state = ExecutionState(graph.tasks.keys())
        executor = _ScriptedExecutor()

        async def worker_a() -> None:
            handle = current_proposals()
            handle.propose_task(_spec("N"))
            handle.propose_dependency("N", "B")

        executor.scripts["A"] = worker_a
        summary = await Scheduler(graph, executor, state).run()

        assert graph.dependencies["B"] == {"A", "N"}
        assert executor.before(("finish", "A"), ("start", "N"))
        assert executor.before(("finish", "N"), ("start", "B"))
        assert summary.completed == ("A", "B", "N")
        assert [outcome.applied for outcome in summary.proposals] == [True, True]

    asyncio.run(scenario())


def test_task_proposed_with_prerequisites_cannot_start_early() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A")
        state = ExecutionState(graph.tasks.keys())
        executor = _ScriptedExecutor()
        a_done = asyncio.Event()

        async def worker_a() -> None:
            handle = current_proposals()
            handle.propose_task(_spec("T"), prerequisites=("A",))
            handle.propose_task(_spec("P"))
            await a_done.wait()

        executor.scripts["A"] = worker_a
        run_task = asyncio.create_task(Scheduler(graph, executor, state).run())

        # P has no prerequisites, so its start proves the scheduler drained
        # both proposals and rechecked readiness while A is still running
        await executor.started("P").wait()

        assert graph.dependencies["T"] == {"A"}
        assert "T" in graph.dependents["A"]
        assert state.state_of("T") is TaskState.PENDING
        assert state.state_of("A") is TaskState.RUNNING
        assert "T" not in executor.start_order

        a_done.set()
        summary = await run_task

        assert executor.before(("finish", "A"), ("start", "T"))
        assert summary.completed == ("A", "P", "T")

    asyncio.run(scenario())


def test_one_workers_proposals_apply_in_submission_order() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "Y")
        graph.add_dependency("A", "Y")
        executor = _ScriptedExecutor()

        async def worker_a() -> None:
            handle = current_proposals()
            # the edge names X, so it applies only after the task proposal
            handle.propose_task(_spec("X"))
            handle.propose_dependency("X", "Y")

        executor.scripts["A"] = worker_a
        summary = await Scheduler(graph, executor).run()

        assert [
            (type(outcome.proposal), outcome.applied)
            for outcome in summary.proposals
        ] == [(AddTaskProposal, True), (AddDependencyProposal, True)]
        assert graph.dependencies["Y"] == {"A", "X"}
        assert executor.before(("finish", "X"), ("start", "Y"))
        assert summary.completed == ("A", "X", "Y")

    asyncio.run(scenario())


def test_a_duplicate_proposal_is_rejected_and_the_drain_continues() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A")
        executor = _ScriptedExecutor()
        first = _spec("Z", "first")

        async def worker_a() -> None:
            handle = current_proposals()
            handle.propose_task(first)
            handle.propose_task(_spec("Z", "second"))
            handle.propose_task(_spec("W"))

        executor.scripts["A"] = worker_a
        summary = await Scheduler(graph, executor).run()

        outcomes = summary.proposals
        assert [outcome.proposal.task.id for outcome in outcomes] == ["Z", "Z", "W"]
        assert [outcome.applied for outcome in outcomes] == [True, False, True]
        assert outcomes[1].reason is not None
        assert outcomes[1].reason.startswith("DuplicateTaskError: ")
        assert graph.tasks["Z"] is first
        assert summary.completed == ("A", "W", "Z")

    asyncio.run(scenario())


def test_proposals_from_two_workers_apply_in_submission_order() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B")
        executor = _ScriptedExecutor()
        a_proposed = asyncio.Event()
        b_proposed = asyncio.Event()

        async def worker_a() -> None:
            current_proposals().propose_task(_spec("A1"))
            a_proposed.set()
            await b_proposed.wait()
            current_proposals().propose_task(_spec("A2"))

        async def worker_b() -> None:
            await a_proposed.wait()
            current_proposals().propose_task(_spec("B1"))
            b_proposed.set()

        executor.scripts["A"] = worker_a
        executor.scripts["B"] = worker_b
        summary = await Scheduler(graph, executor).run()

        assert [
            (outcome.proposal.proposed_by, outcome.proposal.task.id)
            for outcome in summary.proposals
        ] == [("A", "A1"), ("B", "B1"), ("A", "A2")]
        assert all(outcome.applied for outcome in summary.proposals)
        assert summary.completed == ("A", "A1", "A2", "B", "B1")

    asyncio.run(scenario())


def test_a_proposal_from_the_last_running_worker_is_still_applied() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A")
        executor = _ScriptedExecutor()

        async def worker_a() -> None:
            current_proposals().propose_task(_spec("X"))

        executor.scripts["A"] = worker_a
        summary = await Scheduler(graph, executor).run()

        assert executor.start_order == ["A", "X"]
        assert summary.completed == ("A", "X")
        assert len(summary.proposals) == 1
        assert summary.proposals[0].applied is True

    asyncio.run(scenario())


def test_a_task_left_running_by_a_worker_cannot_propose_after_it_settles() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A")
        executor = _ScriptedExecutor()
        release = asyncio.Event()
        errors: list[ProposalContextError] = []
        leftovers: list[asyncio.Task[None]] = []

        async def late_proposal() -> None:
            # copies the worker's context, so it sees the worker's handle
            await release.wait()
            try:
                current_proposals().propose_task(_spec("X"))
            except ProposalContextError as error:
                errors.append(error)

        async def worker_a() -> None:
            leftovers.append(asyncio.create_task(late_proposal()))

        executor.scripts["A"] = worker_a
        scheduler = Scheduler(graph, executor)
        summary = await scheduler.run()

        release.set()
        await leftovers[0]

        assert len(errors) == 1
        assert summary.proposals == ()

        second = await scheduler.run()

        assert second.proposals == ()
        assert "X" not in graph.tasks

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("propose", "error"),
    [
        (lambda handle: handle.propose_dependency("C", "B"), CycleError),
        (lambda handle: handle.propose_dependency("B", "B"), CycleError),
        (
            lambda handle: handle.propose_task(_spec("N"), prerequisites=("N",)),
            CycleError,
        ),
        (lambda handle: handle.propose_dependency("D", "A"), TaskStartedError),
        (lambda handle: handle.propose_dependency("A", "D"), TaskStartedError),
        (
            lambda handle: handle.propose_task(_spec("B", "replacement")),
            DuplicateTaskError,
        ),
        (lambda handle: handle.propose_dependency("missing", "B"), MissingTaskError),
        (lambda handle: handle.propose_dependency("B", "missing"), MissingTaskError),
        (
            lambda handle: handle.propose_task(
                _spec("N"), prerequisites=("A", "missing")
            ),
            MissingTaskError,
        ),
    ],
    ids=[
        "cycle-closing-a-path",
        "cycle-self-edge",
        "cycle-self-prerequisite",
        "started-running-proposer",
        "started-completed",
        "duplicate-task",
        "missing-prerequisite",
        "missing-dependent",
        "task-with-a-missing-prerequisite",
    ],
)
def test_a_rejected_proposal_changes_nothing_and_spares_the_worker(
    propose: Callable[[ProposalHandle], MutationProposal], error: type[Exception]
) -> None:
    async def scenario() -> None:
        # A runs with B and C pending behind it; D completed before the run
        graph = TaskGraph()
        _add_tasks(graph, "A", "B", "C", "D")
        graph.add_dependency("A", "B")
        graph.add_dependency("B", "C")
        state = ExecutionState(graph.tasks.keys())
        for next_state in (TaskState.READY, TaskState.RUNNING, TaskState.COMPLETED):
            state.transition("D", next_state)
        original_b = graph.tasks["B"]
        probe = _spec("P")
        executor = _ScriptedExecutor()
        seen: dict[str, object] = {}

        async def worker_a() -> None:
            handle = current_proposals()
            seen["topology"] = _topology(graph)
            seen["states"] = state.snapshot()
            seen["proposal"] = propose(handle)
            # reached only because the rejection is not raised into the worker
            seen["after_propose"] = True
            # the probe applies only after the rejected proposal in the drain
            handle.propose_task(probe)
            await executor.started("P").wait()

        async def worker_p() -> None:
            # A is still running and nothing else changed since submission
            seen["probe_topology"] = _topology(graph)
            seen["probe_states"] = state.snapshot()

        executor.scripts["A"] = worker_a
        executor.scripts["P"] = worker_p
        summary = await Scheduler(graph, executor, state).run()

        tasks, dependencies, dependents = seen["topology"]
        assert seen["probe_topology"] == (
            {**tasks, "P": probe},
            {**dependencies, "P": set()},
            {**dependents, "P": set()},
        )
        assert seen["probe_states"] == {
            **seen["states"],
            "P": TaskState.RUNNING,
        }
        assert seen["states"] == {
            "A": TaskState.RUNNING,
            "B": TaskState.PENDING,
            "C": TaskState.PENDING,
            "D": TaskState.COMPLETED,
        }
        assert "N" not in graph.tasks
        assert "N" not in summary.states
        assert graph.tasks["B"] is original_b
        assert seen["after_propose"] is True

        assert summary.completed == ("A", "B", "C", "D", "P")
        assert summary.failed == ()
        assert summary.blocked == ()
        rejected, probe_outcome = summary.proposals
        assert rejected.proposal is seen["proposal"]
        assert rejected.applied is False
        assert rejected.reason is not None
        assert rejected.reason.startswith(f"{error.__name__}: ")
        assert probe_outcome.applied is True

    asyncio.run(scenario())


def test_run_summary_exposes_outcomes_in_application_order() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B")
        graph.add_dependency("A", "B")
        executor = _ScriptedExecutor()
        submitted: list[MutationProposal] = []

        async def worker_a() -> None:
            handle = current_proposals()
            submitted.append(handle.propose_task(_spec("X")))
            submitted.append(handle.propose_dependency("B", "A"))
            submitted.append(handle.propose_dependency("X", "B"))
            # stay running until the drain has started X
            await executor.started("X").wait()

        executor.scripts["A"] = worker_a
        summary = await Scheduler(graph, executor).run()

        assert all(
            isinstance(outcome, ProposalOutcome) for outcome in summary.proposals
        )
        assert [outcome.proposal for outcome in summary.proposals] == submitted
        assert all(
            outcome.proposal is proposal
            for outcome, proposal in zip(summary.proposals, submitted, strict=True)
        )
        applied_x, rejected_edge, applied_edge = summary.proposals
        assert (applied_x.applied, applied_x.reason) == (True, None)
        assert rejected_edge.applied is False
        assert rejected_edge.reason == (
            "TaskStartedError: dependent already started: A is RUNNING"
        )
        assert (applied_edge.applied, applied_edge.reason) == (True, None)
        assert summary.completed == ("A", "B", "X")

    asyncio.run(scenario())


def test_run_summary_has_no_outcomes_without_proposals() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B")
        graph.add_dependency("A", "B")

        summary = await Scheduler(graph, _ScriptedExecutor()).run()

        assert summary.proposals == ()
        assert summary.completed == ("A", "B")

    asyncio.run(scenario())


def test_a_failed_workers_proposal_is_still_applied() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A")
        executor = _ScriptedExecutor()

        async def worker_a() -> None:
            current_proposals().propose_task(_spec("X"))
            raise RuntimeError("worker failed: A")

        executor.scripts["A"] = worker_a
        summary = await Scheduler(graph, executor).run()

        assert summary.failed == ("A",)
        assert summary.completed == ("X",)
        assert executor.start_order == ["A", "X"]
        assert len(summary.proposals) == 1
        assert summary.proposals[0].applied is True

    asyncio.run(scenario())
