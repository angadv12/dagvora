import asyncio
from collections.abc import Mapping

import pytest

from dagvora.exceptions import (
    CycleError,
    DuplicateTaskError,
    MissingTaskError,
    TaskStartedError,
)
from dagvora.graph import TaskGraph
from dagvora.models import TaskSpec
from dagvora.scheduler import Scheduler
from dagvora.state import ExecutionState, TaskState


class _RecordingExecutor:
    def __init__(
        self,
        gates: Mapping[str, asyncio.Event] | None = None,
        start_signals: Mapping[str, asyncio.Event] | None = None,
        failures: set[str] | None = None,
    ) -> None:
        self.gates = dict(gates or {})
        self.start_signals = dict(start_signals or {})
        self.failures = set(failures or ())
        self.start_order: list[str] = []
        self.finished_order: list[str] = []
        self.events: list[tuple[str, str]] = []

    async def execute(self, spec: TaskSpec) -> None:
        task_id = spec.id
        self.start_order.append(task_id)
        self.events.append(("start", task_id))
        start_signal = self.start_signals.get(task_id)
        if start_signal is not None:
            start_signal.set()

        try:
            gate = self.gates.get(task_id)
            if gate is not None:
                await gate.wait()
            if task_id in self.failures:
                raise RuntimeError(f"worker failed: {task_id}")
        finally:
            self.finished_order.append(task_id)
            self.events.append(("finish", task_id))


def _add_tasks(graph: TaskGraph, *task_ids: str) -> None:
    for task_id in task_ids:
        graph.add_task(TaskSpec(id=task_id, title=task_id))


def _topology(
    graph: TaskGraph,
) -> tuple[dict[str, TaskSpec], dict[str, set[str]], dict[str, set[str]]]:
    return (
        dict(graph.tasks),
        {task_id: set(ids) for task_id, ids in graph.dependencies.items()},
        {task_id: set(ids) for task_id, ids in graph.dependents.items()},
    )


def test_scheduler_does_not_start_dependent_before_prerequisite_completes() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B")
        graph.add_dependency("A", "B")

        prerequisite_done = asyncio.Event()
        prerequisite_started = asyncio.Event()
        executor = _RecordingExecutor(
            gates={"A": prerequisite_done},
            start_signals={"A": prerequisite_started},
        )
        run_task = asyncio.create_task(Scheduler(graph, executor).run())

        await prerequisite_started.wait()
        assert executor.start_order == ["A"]

        prerequisite_done.set()
        summary = await run_task

        assert summary.completed == ("A", "B")

    asyncio.run(scenario())


def test_scheduler_runs_a_linear_chain_in_dependency_order() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B", "C")
        graph.add_dependency("A", "B")
        graph.add_dependency("B", "C")

        gates = {task_id: asyncio.Event() for task_id in ("A", "B", "C")}
        started = {task_id: asyncio.Event() for task_id in ("A", "B", "C")}
        executor = _RecordingExecutor(gates=gates, start_signals=started)
        run_task = asyncio.create_task(Scheduler(graph, executor).run())

        await started["A"].wait()
        assert executor.start_order == ["A"]
        gates["A"].set()

        await started["B"].wait()
        assert executor.start_order == ["A", "B"]
        gates["B"].set()

        await started["C"].wait()
        assert executor.start_order == ["A", "B", "C"]
        gates["C"].set()

        await run_task
        assert executor.start_order == ["A", "B", "C"]

    asyncio.run(scenario())


def test_scheduler_runs_diamond_branches_concurrently() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B", "C", "D")
        graph.add_dependency("A", "B")
        graph.add_dependency("A", "C")
        graph.add_dependency("B", "D")
        graph.add_dependency("C", "D")

        prerequisite_done = asyncio.Event()
        a_started = asyncio.Event()
        b_started = asyncio.Event()
        c_started = asyncio.Event()
        executor = _RecordingExecutor(
            gates={
                "A": prerequisite_done,
                "B": c_started,
                "C": b_started,
            },
            start_signals={
                "A": a_started,
                "B": b_started,
                "C": c_started,
            },
        )
        run_task = asyncio.create_task(Scheduler(graph, executor).run())

        await a_started.wait()
        prerequisite_done.set()
        await b_started.wait()
        await c_started.wait()

        summary = await run_task

        assert executor.events.index(("finish", "B")) < executor.events.index(
            ("start", "D")
        )
        assert executor.events.index(("finish", "C")) < executor.events.index(
            ("start", "D")
        )
        assert summary.completed == ("A", "B", "C", "D")

    asyncio.run(scenario())


def test_scheduler_starts_downstream_before_long_work_finishes() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "S", "L", "D")
        graph.add_dependency("S", "D")

        long_started = asyncio.Event()
        dependent_started = asyncio.Event()
        executor = _RecordingExecutor(
            gates={"L": dependent_started},
            start_signals={
                "L": long_started,
                "D": dependent_started,
            },
        )
        run_task = asyncio.create_task(Scheduler(graph, executor).run())

        await long_started.wait()
        await dependent_started.wait()
        summary = await run_task

        assert executor.events.index(("start", "D")) < executor.events.index(
            ("finish", "L")
        )
        assert summary.completed == ("D", "L", "S")

    asyncio.run(scenario())


def test_scheduler_contains_worker_failure_in_summary() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A")
        executor = _RecordingExecutor(failures={"A"})

        summary = await Scheduler(graph, executor).run()

        assert summary.failed == ("A",)
        assert summary.states == {"A": TaskState.FAILED}

    asyncio.run(scenario())


def test_scheduler_continues_a_disjoint_branch_after_failure() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B", "C", "D")
        graph.add_dependency("A", "B")
        graph.add_dependency("C", "D")
        executor = _RecordingExecutor(failures={"A"})

        summary = await Scheduler(graph, executor).run()

        assert summary.completed == ("C", "D")
        assert summary.failed == ("A",)

    asyncio.run(scenario())


def test_scheduler_reports_dependents_of_failed_task_as_blocked() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B", "C")
        graph.add_dependency("A", "B")
        graph.add_dependency("B", "C")
        executor = _RecordingExecutor(failures={"A"})

        summary = await Scheduler(graph, executor).run()

        assert summary.blocked == ("B", "C")
        assert summary.failed == ("A",)
        assert summary.states == {
            "A": TaskState.FAILED,
            "B": TaskState.PENDING,
            "C": TaskState.PENDING,
        }

    asyncio.run(scenario())


def test_scheduler_terminates_on_an_empty_graph() -> None:
    async def scenario() -> None:
        summary = await Scheduler(TaskGraph(), _RecordingExecutor()).run()

        assert summary.completed == ()
        assert summary.failed == ()
        assert summary.blocked == ()
        assert summary.states == {}

    asyncio.run(scenario())


def test_scheduler_terminates_when_all_remaining_tasks_are_blocked() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B", "C")
        graph.add_dependency("A", "B")
        graph.add_dependency("B", "C")
        executor = _RecordingExecutor(failures={"A"})

        summary = await Scheduler(graph, executor).run()

        assert summary.blocked == ("B", "C")

    asyncio.run(scenario())


def test_scheduler_summary_has_sorted_disjoint_groups_and_final_states() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "z", "m", "b", "a")
        graph.add_dependency("z", "m")
        executor = _RecordingExecutor(failures={"z"})

        summary = await Scheduler(graph, executor).run()

        assert summary.states == {
            "z": TaskState.FAILED,
            "m": TaskState.PENDING,
            "b": TaskState.COMPLETED,
            "a": TaskState.COMPLETED,
        }
        assert summary.completed == ("a", "b")
        assert summary.failed == ("z",)
        assert summary.blocked == ("m",)
        assert summary.completed == tuple(sorted(summary.completed))
        assert summary.failed == tuple(sorted(summary.failed))
        assert summary.blocked == tuple(sorted(summary.blocked))
        assert set(summary.completed).isdisjoint(summary.failed)
        assert set(summary.completed).isdisjoint(summary.blocked)
        assert set(summary.failed).isdisjoint(summary.blocked)

    asyncio.run(scenario())


def test_scheduler_runs_a_task_added_mid_run() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A")
        state = ExecutionState(graph.tasks.keys())

        a_started = asyncio.Event()
        x_started = asyncio.Event()
        # A cannot finish until X starts, so X must start while A is running
        executor = _RecordingExecutor(
            gates={"A": x_started},
            start_signals={"A": a_started, "X": x_started},
        )
        scheduler = Scheduler(graph, executor, state)
        run_task = asyncio.create_task(scheduler.run())

        await a_started.wait()
        spec = TaskSpec(id="X", title="X")
        scheduler.add_task(spec)

        assert graph.tasks["X"] is spec
        assert graph.dependencies["X"] == set()
        assert state.state_of("X") is TaskState.PENDING
        assert state.state_of("A") is TaskState.RUNNING

        summary = await run_task

        assert executor.start_order == ["A", "X"]
        assert executor.events.index(("start", "X")) < executor.events.index(
            ("finish", "A")
        )
        assert summary.completed == ("A", "X")

    asyncio.run(scenario())


def test_scheduler_rejects_a_duplicate_task_mid_run_without_changes() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B")
        graph.add_dependency("A", "B")
        state = ExecutionState(graph.tasks.keys())
        original = graph.tasks["A"]

        a_started = asyncio.Event()
        a_done = asyncio.Event()
        executor = _RecordingExecutor(
            gates={"A": a_done}, start_signals={"A": a_started}
        )
        scheduler = Scheduler(graph, executor, state)
        run_task = asyncio.create_task(scheduler.run())

        await a_started.wait()
        topology_before = _topology(graph)
        states_before = state.snapshot()

        with pytest.raises(DuplicateTaskError):
            scheduler.add_task(TaskSpec(id="A", title="replacement"))
        with pytest.raises(DuplicateTaskError):
            scheduler.add_task(TaskSpec(id="B", title="replacement"))

        assert _topology(graph) == topology_before
        assert graph.tasks["A"] is original
        assert state.snapshot() == states_before

        a_done.set()
        summary = await run_task

        assert executor.start_order == ["A", "B"]
        assert summary.completed == ("A", "B")

    asyncio.run(scenario())


def test_add_task_rejects_an_id_tracked_only_by_state_without_changes() -> None:
    graph = TaskGraph()
    state = ExecutionState(("ghost",))
    scheduler = Scheduler(graph, _RecordingExecutor(), state)

    with pytest.raises(DuplicateTaskError):
        scheduler.add_task(TaskSpec(id="ghost", title="ghost"))

    assert graph.tasks == {}
    assert state.snapshot() == {"ghost": TaskState.PENDING}


def test_scheduler_holds_a_dependent_for_a_dependency_added_mid_run() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B", "C", "D")
        graph.add_dependency("A", "B")
        graph.add_dependency("A", "D")
        state = ExecutionState(graph.tasks.keys())

        a_started = asyncio.Event()
        c_started = asyncio.Event()
        d_started = asyncio.Event()
        a_done = asyncio.Event()
        c_done = asyncio.Event()
        executor = _RecordingExecutor(
            gates={"A": a_done, "C": c_done},
            start_signals={"A": a_started, "C": c_started, "D": d_started},
        )
        scheduler = Scheduler(graph, executor, state)
        run_task = asyncio.create_task(scheduler.run())

        await a_started.wait()
        await c_started.wait()
        scheduler.add_dependency("C", "B")

        assert graph.dependencies["B"] == {"A", "C"}
        assert graph.dependents["C"] == {"B"}
        assert state.state_of("B") is TaskState.PENDING
        assert state.state_of("C") is TaskState.RUNNING

        a_done.set()
        # D shares A as its only prerequisite, so its start proves the
        # scheduler settled A and rechecked readiness
        await d_started.wait()

        assert state.state_of("B") is TaskState.PENDING
        assert "B" not in executor.start_order
        assert state.state_of("C") is TaskState.RUNNING

        c_done.set()
        summary = await run_task

        assert executor.start_order.count("C") == 1
        assert executor.events.index(("finish", "C")) < executor.events.index(
            ("start", "B")
        )
        assert summary.completed == ("A", "B", "C", "D")

    asyncio.run(scenario())


def test_scheduler_runs_a_subgraph_added_mid_run() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A")
        state = ExecutionState(graph.tasks.keys())

        a_started = asyncio.Event()
        p_started = asyncio.Event()
        a_done = asyncio.Event()
        executor = _RecordingExecutor(
            gates={"A": a_done},
            start_signals={"A": a_started, "P": p_started},
        )
        scheduler = Scheduler(graph, executor, state)
        run_task = asyncio.create_task(scheduler.run())

        await a_started.wait()
        for task_id in ("X", "Y", "P"):
            scheduler.add_task(TaskSpec(id=task_id, title=task_id))
        scheduler.add_dependency("A", "X")
        scheduler.add_dependency("X", "Y")

        # P has no prerequisites, so its start proves the scheduler woke and
        # rechecked readiness with the new edges in place
        await p_started.wait()

        assert state.state_of("A") is TaskState.RUNNING
        assert state.state_of("X") is TaskState.PENDING
        assert state.state_of("Y") is TaskState.PENDING
        assert executor.start_order == ["A", "P"]

        a_done.set()
        summary = await run_task

        events = executor.events
        assert events.index(("finish", "A")) < events.index(("start", "X"))
        assert events.index(("finish", "X")) < events.index(("start", "Y"))
        assert summary.completed == ("A", "P", "X", "Y")

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("prerequisite_id", "dependent_id", "dependent_state"),
    [
        ("C", "R", TaskState.RUNNING),
        ("R", "C", TaskState.COMPLETED),
        ("C", "F", TaskState.FAILED),
    ],
)
def test_scheduler_rejects_a_dependency_on_started_work_without_changes(
    prerequisite_id: str, dependent_id: str, dependent_state: TaskState
) -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "C", "D", "F", "R")
        graph.add_dependency("C", "D")
        state = ExecutionState(graph.tasks.keys())

        f_started = asyncio.Event()
        d_started = asyncio.Event()
        c_done = asyncio.Event()
        r_done = asyncio.Event()
        executor = _RecordingExecutor(
            gates={"C": c_done, "R": r_done},
            start_signals={"F": f_started, "D": d_started},
            failures={"F"},
        )
        scheduler = Scheduler(graph, executor, state)
        run_task = asyncio.create_task(scheduler.run())

        # F raises in the step that signals its start, so it is done before
        # C is released; D starting then proves both C and F were settled
        await f_started.wait()
        c_done.set()
        await d_started.wait()

        assert state.state_of(dependent_id) is dependent_state
        topology_before = _topology(graph)
        states_before = state.snapshot()

        with pytest.raises(TaskStartedError):
            scheduler.add_dependency(prerequisite_id, dependent_id)

        assert _topology(graph) == topology_before
        assert state.snapshot() == states_before

        r_done.set()
        summary = await run_task

        assert sorted(executor.start_order) == ["C", "D", "F", "R"]
        assert summary.completed == ("C", "D", "R")
        assert summary.failed == ("F",)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("prerequisite_id", "dependent_id", "error"),
    [
        ("C", "B", CycleError),
        ("B", "B", CycleError),
        ("missing", "B", MissingTaskError),
        ("B", "missing", MissingTaskError),
    ],
)
def test_scheduler_rejects_an_invalid_dependency_mid_run_without_changes(
    prerequisite_id: str, dependent_id: str, error: type[Exception]
) -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B", "C")
        graph.add_dependency("A", "B")
        graph.add_dependency("B", "C")
        state = ExecutionState(graph.tasks.keys())

        a_started = asyncio.Event()
        a_done = asyncio.Event()
        executor = _RecordingExecutor(
            gates={"A": a_done}, start_signals={"A": a_started}
        )
        scheduler = Scheduler(graph, executor, state)
        run_task = asyncio.create_task(scheduler.run())

        await a_started.wait()
        topology_before = _topology(graph)
        states_before = state.snapshot()

        with pytest.raises(error):
            scheduler.add_dependency(prerequisite_id, dependent_id)

        assert _topology(graph) == topology_before
        assert state.snapshot() == states_before

        a_done.set()
        summary = await run_task

        assert executor.start_order == ["A", "B", "C"]
        assert summary.completed == ("A", "B", "C")

    asyncio.run(scenario())


def test_add_dependency_returns_a_ready_dependent_to_pending() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B")
        state = ExecutionState(graph.tasks.keys())
        state.transition("B", TaskState.READY)
        executor = _RecordingExecutor()
        scheduler = Scheduler(graph, executor, state)

        scheduler.add_dependency("A", "B")

        assert graph.dependencies["B"] == {"A"}
        assert state.state_of("B") is TaskState.PENDING

        summary = await scheduler.run()

        assert executor.events.index(("finish", "A")) < executor.events.index(
            ("start", "B")
        )
        assert summary.completed == ("A", "B")

    asyncio.run(scenario())


def test_add_dependency_keeps_a_ready_dependent_with_a_completed_prerequisite() -> (
    None
):
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B")
        state = ExecutionState(graph.tasks.keys())
        for next_state in (TaskState.READY, TaskState.RUNNING, TaskState.COMPLETED):
            state.transition("A", next_state)
        state.transition("B", TaskState.READY)
        executor = _RecordingExecutor()
        scheduler = Scheduler(graph, executor, state)

        scheduler.add_dependency("A", "B")

        assert graph.dependencies["B"] == {"A"}
        assert state.state_of("B") is TaskState.READY

        summary = await scheduler.run()

        assert executor.start_order == ["B"]
        assert summary.completed == ("A", "B")

    asyncio.run(scenario())


def test_scheduler_runs_a_task_added_mid_run_with_a_prerequisite() -> None:
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A")
        state = ExecutionState(graph.tasks.keys())

        a_started = asyncio.Event()
        p_started = asyncio.Event()
        a_done = asyncio.Event()
        executor = _RecordingExecutor(
            gates={"A": a_done},
            start_signals={"A": a_started, "P": p_started},
        )
        scheduler = Scheduler(graph, executor, state)
        run_task = asyncio.create_task(scheduler.run())

        await a_started.wait()
        scheduler.add_task(TaskSpec(id="T", title="T"), prerequisites=("A",))
        scheduler.add_task(TaskSpec(id="P", title="P"))

        assert graph.dependencies["T"] == {"A"}
        assert "T" in graph.dependents["A"]
        assert state.state_of("T") is TaskState.PENDING

        # P has no prerequisites, so its start proves the scheduler woke and
        # rechecked readiness with the edge into T in place
        await p_started.wait()

        assert state.state_of("A") is TaskState.RUNNING
        assert state.state_of("T") is TaskState.PENDING
        assert "T" not in executor.start_order

        a_done.set()
        summary = await run_task

        assert executor.events.index(("finish", "A")) < executor.events.index(
            ("start", "T")
        )
        assert summary.completed == ("A", "P", "T")

    asyncio.run(scenario())


def test_scheduler_holds_a_task_added_mid_run_until_all_prerequisites_finish() -> (
    None
):
    async def scenario() -> None:
        graph = TaskGraph()
        _add_tasks(graph, "A", "B")
        state = ExecutionState(graph.tasks.keys())

        a_started = asyncio.Event()
        b_started = asyncio.Event()
        p_started = asyncio.Event()
        a_done = asyncio.Event()
        b_done = asyncio.Event()
        executor = _RecordingExecutor(
            gates={"A": a_done, "B": b_done},
            start_signals={"A": a_started, "B": b_started, "P": p_started},
        )
        scheduler = Scheduler(graph, executor, state)
        run_task = asyncio.create_task(scheduler.run())

        await a_started.wait()
        await b_started.wait()
        scheduler.add_task(TaskSpec(id="T", title="T"), prerequisites=("A", "B"))
        scheduler.add_task(TaskSpec(id="P", title="P"), prerequisites=("B",))

        assert graph.dependencies["T"] == {"A", "B"}
        assert graph.dependencies["P"] == {"B"}

        b_done.set()
        # P waits only on B, so its start proves B was settled and readiness
        # was rechecked while A is still running
        await p_started.wait()

        assert state.state_of("B") is TaskState.COMPLETED
        assert state.state_of("A") is TaskState.RUNNING
        assert state.state_of("T") is TaskState.PENDING
        assert "T" not in executor.start_order

        a_done.set()
        summary = await run_task

        events = executor.events
        assert events.index(("finish", "B")) < events.index(("finish", "A"))
        assert events.index(("finish", "A")) < events.index(("start", "T"))
        assert summary.completed == ("A", "B", "P", "T")

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("task_id", "prerequisites", "error"),
    [
        ("A", ("B",), DuplicateTaskError),
        ("T", ("missing",), MissingTaskError),
        ("T", ("A", "missing"), MissingTaskError),
        ("T", ("T",), CycleError),
        ("ghost", ("A",), DuplicateTaskError),
    ],
)
def test_add_task_with_prerequisites_rejects_without_changes(
    task_id: str, prerequisites: tuple[str, ...], error: type[Exception]
) -> None:
    graph = TaskGraph()
    _add_tasks(graph, "A", "B")
    graph.add_dependency("A", "B")
    # ghost is tracked by state only, so graph checks alone cannot catch it
    state = ExecutionState(("A", "B", "ghost"))
    scheduler = Scheduler(graph, _RecordingExecutor(), state)
    topology_before = _topology(graph)
    states_before = state.snapshot()

    with pytest.raises(error):
        scheduler.add_task(
            TaskSpec(id=task_id, title=task_id), prerequisites=prerequisites
        )

    assert _topology(graph) == topology_before
    assert state.snapshot() == states_before
