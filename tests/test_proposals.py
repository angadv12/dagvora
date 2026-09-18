import asyncio

import pytest
from pydantic import ValidationError

from dagvora.exceptions import ProposalContextError
from dagvora.models import TaskSpec
from dagvora.proposals import (
    AddDependencyProposal,
    AddTaskProposal,
    ProposalHandle,
    ProposalQueue,
    bind_proposals,
    current_proposals,
)


def _spec(task_id: str) -> TaskSpec:
    return TaskSpec(id=task_id, title=task_id)


def test_add_task_proposal_records_proposer_task_and_prerequisites() -> None:
    proposal = AddTaskProposal(
        proposed_by="A", task=_spec("B"), prerequisites=("A", "C")
    )

    assert proposal.proposed_by == "A"
    assert proposal.task == _spec("B")
    assert proposal.prerequisites == ("A", "C")


def test_add_dependency_proposal_records_proposer_and_edge() -> None:
    proposal = AddDependencyProposal(
        proposed_by="A", prerequisite_id="B", dependent_id="C"
    )

    assert proposal.proposed_by == "A"
    assert proposal.prerequisite_id == "B"
    assert proposal.dependent_id == "C"


def test_add_task_proposal_prerequisites_default_to_empty_tuple() -> None:
    proposal = AddTaskProposal(proposed_by="A", task=_spec("B"))

    assert proposal.prerequisites == ()


def test_add_task_proposal_stores_list_of_prerequisites_as_tuple() -> None:
    proposal = AddTaskProposal(
        proposed_by="A", task=_spec("B"), prerequisites=["A", "C"]
    )

    assert proposal.prerequisites == ("A", "C")
    assert isinstance(proposal.prerequisites, tuple)


def test_proposals_are_frozen() -> None:
    task_proposal = AddTaskProposal(proposed_by="A", task=_spec("B"))
    dependency_proposal = AddDependencyProposal(
        proposed_by="A", prerequisite_id="B", dependent_id="C"
    )

    with pytest.raises(ValidationError):
        task_proposal.proposed_by = "Z"
    with pytest.raises(ValidationError):
        dependency_proposal.dependent_id = "Z"

    assert task_proposal.proposed_by == "A"
    assert dependency_proposal.dependent_id == "C"


@pytest.mark.parametrize(
    "fields",
    [
        {"proposed_by": ""},
        {"prerequisites": ("A", "")},
    ],
)
def test_add_task_proposal_rejects_empty_ids(fields: dict[str, object]) -> None:
    values: dict[str, object] = {"proposed_by": "A", "task": _spec("B")}
    values.update(fields)

    with pytest.raises(ValidationError):
        AddTaskProposal(**values)


@pytest.mark.parametrize(
    "fields",
    [
        {"proposed_by": ""},
        {"prerequisite_id": ""},
        {"dependent_id": ""},
    ],
)
def test_add_dependency_proposal_rejects_empty_ids(
    fields: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "proposed_by": "A",
        "prerequisite_id": "B",
        "dependent_id": "C",
    }
    values.update(fields)

    with pytest.raises(ValidationError):
        AddDependencyProposal(**values)


def test_queue_drains_in_submission_order_and_empties() -> None:
    queue = ProposalQueue()
    first = AddTaskProposal(proposed_by="A", task=_spec("B"))
    second = AddDependencyProposal(
        proposed_by="A", prerequisite_id="B", dependent_id="C"
    )
    third = AddTaskProposal(proposed_by="C", task=_spec("D"))

    queue.submit(first)
    queue.submit(second)
    queue.submit(third)

    assert queue.drain() == [first, second, third]
    assert len(queue) == 0
    assert queue.drain() == []


def test_queue_length_tracks_queued_proposals() -> None:
    queue = ProposalQueue()
    assert len(queue) == 0

    queue.submit(AddTaskProposal(proposed_by="A", task=_spec("B")))
    assert len(queue) == 1

    queue.submit(AddTaskProposal(proposed_by="A", task=_spec("C")))
    assert len(queue) == 2

    queue.drain()
    assert len(queue) == 0


def test_queue_calls_on_submit_once_per_submit_after_queueing() -> None:
    seen_lengths: list[int] = []
    queue = ProposalQueue(on_submit=lambda: seen_lengths.append(len(queue)))

    queue.submit(AddTaskProposal(proposed_by="A", task=_spec("B")))
    queue.submit(
        AddDependencyProposal(proposed_by="A", prerequisite_id="B", dependent_id="C")
    )

    assert seen_lengths == [1, 2]


def test_queue_accepts_proposals_without_callback() -> None:
    queue = ProposalQueue()
    proposal = AddTaskProposal(proposed_by="A", task=_spec("B"))

    queue.submit(proposal)

    assert queue.drain() == [proposal]


def test_handle_stamps_task_id_and_queues_proposals_in_order() -> None:
    queue = ProposalQueue()
    handle = ProposalHandle("A", queue)

    task_proposal = handle.propose_task(_spec("B"), prerequisites=["A"])
    dependency_proposal = handle.propose_dependency("B", "C")
    bare_task_proposal = handle.propose_task(_spec("D"))

    assert handle.task_id == "A"
    assert task_proposal == AddTaskProposal(
        proposed_by="A", task=_spec("B"), prerequisites=("A",)
    )
    assert dependency_proposal == AddDependencyProposal(
        proposed_by="A", prerequisite_id="B", dependent_id="C"
    )
    assert bare_task_proposal.proposed_by == "A"
    assert bare_task_proposal.prerequisites == ()
    assert queue.drain() == [
        task_proposal,
        dependency_proposal,
        bare_task_proposal,
    ]


def test_handle_task_id_is_read_only() -> None:
    handle = ProposalHandle("A", ProposalQueue())

    with pytest.raises(AttributeError):
        handle.task_id = "B"

    assert handle.task_id == "A"


def test_handle_rejects_invalid_input_without_queueing() -> None:
    submits: list[None] = []
    queue = ProposalQueue(on_submit=lambda: submits.append(None))
    handle = ProposalHandle("A", queue)

    with pytest.raises(ValidationError):
        handle.propose_task(_spec("B"), prerequisites=["C", ""])
    with pytest.raises(ValidationError):
        # a bare string is not a collection of prerequisite ids
        handle.propose_task(_spec("B"), prerequisites="C")
    with pytest.raises(ValidationError):
        handle.propose_dependency("", "C")
    with pytest.raises(ValidationError):
        handle.propose_dependency("B", "")
    with pytest.raises(ValidationError):
        ProposalHandle("", queue).propose_dependency("B", "C")

    assert len(queue) == 0
    assert submits == []


def test_closed_handle_rejects_proposals_without_queueing() -> None:
    submits: list[None] = []
    queue = ProposalQueue(on_submit=lambda: submits.append(None))
    handle = ProposalHandle("A", queue)

    handle.close()

    with pytest.raises(ProposalContextError):
        handle.propose_task(_spec("B"))
    with pytest.raises(ProposalContextError):
        handle.propose_dependency("A", "B")

    assert len(queue) == 0
    assert submits == []


def test_current_proposals_raises_when_nothing_is_bound() -> None:
    with pytest.raises(ProposalContextError):
        current_proposals()

    async def scenario() -> None:
        with pytest.raises(ProposalContextError):
            current_proposals()

    asyncio.run(scenario())


def test_bound_handle_is_visible_only_to_its_own_task() -> None:
    async def scenario() -> None:
        queue = ProposalQueue()
        handle_a = ProposalHandle("A", queue)
        handle_b = ProposalHandle("B", queue)
        a_bound = asyncio.Event()
        b_bound = asyncio.Event()
        seen: dict[str, ProposalHandle] = {}

        async def worker(
            handle: ProposalHandle,
            bound: asyncio.Event,
            other_bound: asyncio.Event,
        ) -> None:
            bind_proposals(handle)
            bound.set()
            # read only after the sibling has bound its own handle
            await other_bound.wait()
            seen[handle.task_id] = current_proposals()
            current_proposals().propose_task(_spec(f"{handle.task_id}-child"))

        await asyncio.gather(
            asyncio.create_task(worker(handle_a, a_bound, b_bound)),
            asyncio.create_task(worker(handle_b, b_bound, a_bound)),
        )

        assert seen["A"] is handle_a
        assert seen["B"] is handle_b
        assert sorted(
            (proposal.proposed_by, proposal.task.id)
            for proposal in queue.drain()
            if isinstance(proposal, AddTaskProposal)
        ) == [("A", "A-child"), ("B", "B-child")]
        # the binding never leaks back into the parent context
        with pytest.raises(ProposalContextError):
            current_proposals()

    asyncio.run(scenario())

    with pytest.raises(ProposalContextError):
        current_proposals()
