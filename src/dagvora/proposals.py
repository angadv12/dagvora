from collections import deque
from collections.abc import Callable, Iterable
from contextvars import ContextVar
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from .exceptions import ProposalContextError
from .models import TaskSpec


class AddTaskProposal(BaseModel):
    model_config = ConfigDict(frozen=True)

    proposed_by: str = Field(min_length=1)
    task: TaskSpec
    prerequisites: tuple[Annotated[str, Field(min_length=1)], ...] = ()


class AddDependencyProposal(BaseModel):
    model_config = ConfigDict(frozen=True)

    proposed_by: str = Field(min_length=1)
    # the edge runs from prerequisite to dependent
    prerequisite_id: str = Field(min_length=1)
    dependent_id: str = Field(min_length=1)


MutationProposal = AddTaskProposal | AddDependencyProposal


class ProposalQueue:
    # holds proposals only; the scheduler validates and applies them
    def __init__(self, on_submit: Callable[[], None] | None = None) -> None:
        self._proposals: deque[MutationProposal] = deque()
        self._on_submit = on_submit

    def submit(self, proposal: MutationProposal) -> None:
        self._proposals.append(proposal)
        if self._on_submit is not None:
            self._on_submit()

    def drain(self) -> list[MutationProposal]:
        drained = list(self._proposals)
        self._proposals.clear()
        return drained

    def __len__(self) -> int:
        return len(self._proposals)


class ProposalHandle:
    # proposed_by comes from the bound task id so a worker cannot speak for
    # another task
    def __init__(self, task_id: str, queue: ProposalQueue) -> None:
        self._task_id = task_id
        self._queue = queue

    @property
    def task_id(self) -> str:
        return self._task_id

    def propose_task(
        self, task: TaskSpec, prerequisites: Iterable[str] = ()
    ) -> AddTaskProposal:
        # pydantic coerces the iterable to a tuple and rejects a bare string
        proposal = AddTaskProposal(
            proposed_by=self._task_id, task=task, prerequisites=prerequisites
        )
        self._queue.submit(proposal)
        return proposal

    def propose_dependency(
        self, prerequisite_id: str, dependent_id: str
    ) -> AddDependencyProposal:
        proposal = AddDependencyProposal(
            proposed_by=self._task_id,
            prerequisite_id=prerequisite_id,
            dependent_id=dependent_id,
        )
        self._queue.submit(proposal)
        return proposal


# each asyncio task copies the context, so a binding is visible only to the
# worker that set it and to anything it awaits or spawns
_current_handle: ContextVar[ProposalHandle | None] = ContextVar(
    "dagvora_proposal_handle", default=None
)


def bind_proposals(handle: ProposalHandle) -> None:
    _current_handle.set(handle)


def current_proposals() -> ProposalHandle:
    handle = _current_handle.get()
    if handle is None:
        raise ProposalContextError(
            "no proposal handle bound: current_proposals() must be called "
            "from a worker started by Scheduler.run"
        )
    return handle
