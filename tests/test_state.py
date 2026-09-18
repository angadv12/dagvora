import pytest

from dagvora.exceptions import (
    DuplicateTaskError,
    InvalidTransitionError,
    MissingTaskError,
)
from dagvora.state import ExecutionState, TaskState


def test_registered_id_defaults_to_pending() -> None:
    state = ExecutionState(("A",))

    assert state.state_of("A") is TaskState.PENDING


@pytest.mark.parametrize(
    ("path", "to_state"),
    [
        ((), TaskState.READY),
        ((TaskState.READY,), TaskState.RUNNING),
        ((TaskState.READY, TaskState.RUNNING), TaskState.COMPLETED),
        ((TaskState.READY, TaskState.RUNNING), TaskState.FAILED),
    ],
)
def test_every_legal_transition_succeeds(
    path: tuple[TaskState, ...], to_state: TaskState
) -> None:
    state = ExecutionState(("A",))

    for next_state in path:
        state.transition("A", next_state)

    state.transition("A", to_state)

    assert state.state_of("A") is to_state


@pytest.mark.parametrize(
    ("current_state", "to_state", "path"),
    [
        (
            TaskState.COMPLETED,
            TaskState.RUNNING,
            (TaskState.READY, TaskState.RUNNING, TaskState.COMPLETED),
        ),
        (TaskState.PENDING, TaskState.RUNNING, ()),
        (TaskState.PENDING, TaskState.COMPLETED, ()),
        (TaskState.READY, TaskState.COMPLETED, (TaskState.READY,)),
        (
            TaskState.FAILED,
            TaskState.PENDING,
            (TaskState.READY, TaskState.RUNNING, TaskState.FAILED),
        ),
        (
            TaskState.FAILED,
            TaskState.READY,
            (TaskState.READY, TaskState.RUNNING, TaskState.FAILED),
        ),
        (
            TaskState.FAILED,
            TaskState.RUNNING,
            (TaskState.READY, TaskState.RUNNING, TaskState.FAILED),
        ),
        (
            TaskState.FAILED,
            TaskState.COMPLETED,
            (TaskState.READY, TaskState.RUNNING, TaskState.FAILED),
        ),
    ],
)
def test_illegal_transition_preserves_state(
    current_state: TaskState,
    to_state: TaskState,
    path: tuple[TaskState, ...],
) -> None:
    state = ExecutionState(("A",))

    for next_state in path:
        state.transition("A", next_state)

    with pytest.raises(InvalidTransitionError):
        state.transition("A", to_state)

    assert state.state_of("A") is current_state


def test_unknown_id_is_rejected_by_state_queries_and_transitions() -> None:
    state = ExecutionState()

    with pytest.raises(MissingTaskError):
        state.state_of("missing")

    with pytest.raises(MissingTaskError):
        state.transition("missing", TaskState.READY)


def test_register_rejects_an_existing_id() -> None:
    state = ExecutionState(("A",))

    with pytest.raises(DuplicateTaskError):
        state.register("A")


def test_ids_in_returns_membership_for_each_state() -> None:
    state = ExecutionState(("pending", "ready", "running", "completed", "failed"))
    state.transition("ready", TaskState.READY)
    state.transition("running", TaskState.READY)
    state.transition("running", TaskState.RUNNING)
    state.transition("completed", TaskState.READY)
    state.transition("completed", TaskState.RUNNING)
    state.transition("completed", TaskState.COMPLETED)
    state.transition("failed", TaskState.READY)
    state.transition("failed", TaskState.RUNNING)
    state.transition("failed", TaskState.FAILED)

    assert state.ids_in(TaskState.PENDING) == {"pending"}
    assert state.ids_in(TaskState.READY) == {"ready"}
    assert state.ids_in(TaskState.RUNNING) == {"running"}
    assert state.ids_in(TaskState.COMPLETED) == {"completed"}
    assert state.ids_in(TaskState.FAILED) == {"failed"}


def test_snapshot_returns_the_full_state_mapping() -> None:
    state = ExecutionState(("A", "B"))
    state.transition("A", TaskState.READY)

    assert state.snapshot() == {
        "A": TaskState.READY,
        "B": TaskState.PENDING,
    }
