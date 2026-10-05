# Dagvora

An agentic software factory built around a runtime-mutable directed acyclic
graph (DAG). Dagvora schedules independent work concurrently and safely adds
new tasks or dependencies as workers discover them.

The current implementation covers the static graph, the execution core, live
graph mutation, and the mutation proposal queue. The LLM planner comes next.

## Design

Three concerns stay separate:

- `TaskSpec` (`dagvora.models`) is the validated description of a task: `id`,
  `title`, and an optional `description`. It is frozen and knows nothing about
  ordering or progress.
- `TaskGraph` (`dagvora.graph`) owns topology: which tasks exist, the
  `prerequisite -> dependent` edges, cycle rejection, and Kahn topological
  sorting.
- `ExecutionState` (`dagvora.state`) owns progress: a task id to `TaskState`
  map and the legal transitions between states.

`Scheduler` (`dagvora.scheduler`) is the only component that reads all three.

## Task states

```
PENDING -> READY -> RUNNING -> COMPLETED
                            -> FAILED
```

`READY -> PENDING` is also legal. It happens only when a runtime dependency gives
a ready task a prerequisite that has not completed. Any other transition raises
`InvalidTransitionError` and leaves the state unchanged. A task becomes ready
when it is `PENDING` and every prerequisite is `COMPLETED`.

## Scheduling

`Scheduler.run()` is an async loop:

1. Apply queued mutation proposals in submission order.
2. Promote every `PENDING` task whose prerequisites are all `COMPLETED` to
   `READY`.
3. Move each `READY` task to `RUNNING` and start it on the executor, tracking
   the in-flight `asyncio.Task` objects explicitly.
4. Stop when nothing is running.
5. Otherwise wait with `asyncio.wait(..., return_when=asyncio.FIRST_COMPLETED)`
   on the running tasks and a wakeup future, and settle each finished task as
   `COMPLETED` or `FAILED`.

Because the loop reacts to the first completion rather than the whole batch,
a task unlocked by a fast prerequisite starts immediately while slower siblings
are still running.

Proposals are applied at the top of every iteration. A finished worker's
proposals are therefore applied after it settles and before the next readiness
check, so its dependents are never promoted ahead of the edges it proposed.
Submitting a proposal resolves the wakeup future, so proposals from workers that
are still running are applied without waiting for any worker to finish. The run
ends only when nothing is running and the proposal queue is empty.

A worker exception marks only that task `FAILED`; it is not re-raised and it
never fails other tasks. Independent branches keep running to completion. The
dependents of a failed task simply never satisfy readiness, so they finish the
run `PENDING` and are reported as `blocked`.

## Runtime mutation

While `run()` is active, the orchestrator changes the graph through the
scheduler, never through `TaskGraph` or `ExecutionState` directly:

```python
scheduler.add_task(
    TaskSpec(id="migrate", title="Write migration"),
    prerequisites=("schema",),
)
scheduler.add_dependency("migrate", "api")
```

- `add_task(spec, *, prerequisites=())` registers the spec in both `TaskGraph`
  and `ExecutionState` as `PENDING` and adds a `prerequisite -> spec.id` edge
  for each listed id. It validates everything before it writes: a duplicate id
  raises `DuplicateTaskError`, an unknown prerequisite raises
  `MissingTaskError`, and listing the task as its own prerequisite raises
  `CycleError`. A rejection is all-or-nothing, so no part of the task or its
  edges is kept.
- `add_dependency` requires a `PENDING` or `READY` dependent. A `RUNNING`,
  `COMPLETED`, or `FAILED` dependent raises `TaskStartedError`. Missing ids and
  cycles are rejected by `TaskGraph.add_dependency` with `MissingTaskError` and
  `CycleError`.
- A rejected mutation leaves the graph and the execution state unchanged.
- A new edge applies from the next readiness check. A `READY` dependent that
  gains an unfinished prerequisite returns to `PENDING`.
- Running work is never cancelled, restarted, or given new prerequisites. A
  running task can still gain new dependents.

Both methods are synchronous. They run on the event loop between awaits, so each
mutation is fully applied before the scheduler resumes. While `run()` waits on
workers it also waits on a wakeup future that `add_task` resolves, so a new task
with no unmet prerequisites starts without waiting for running work to finish.

Pass a new task's prerequisites to `add_task` instead of adding them with later
`add_dependency` calls. One call creates the task and its edges together, so the
scheduler never sees the task without them and it cannot start early. A run ends
when nothing is running; mutations made after that are picked up by the next
`run()`.

## Executors

An executor is any object with an async `execute` method:

```python
class Executor(Protocol):
    async def execute(self, spec: TaskSpec) -> None: ...
```

Returning normally completes the task; raising fails it.

## Mutation proposals

Workers never call `add_task` or `add_dependency`. Inside `Executor.execute`, a
worker calls `current_proposals()` to get its `ProposalHandle`, then submits
proposals with `propose_task(...)` or `propose_dependency(...)`. The scheduler
validates and applies them. The `execute(spec)` protocol is unchanged, so
executors that never propose behave exactly as before.

```python
from dagvora import TaskSpec, current_proposals


class DiscoveringExecutor:
    async def execute(self, spec: TaskSpec) -> None:
        if spec.id == "schema":
            proposals = current_proposals()
            # api must also wait for a migration nobody planned
            proposals.propose_task(
                TaskSpec(id="migrate", title="Write migration"),
                prerequisites=("schema",),
            )
            proposals.propose_dependency("migrate", "api")
```

- The scheduler binds each worker's handle in a `ContextVar` inside that
  worker's own `asyncio.Task`. The handle is visible to the worker and to
  anything it awaits or spawns, never to sibling workers or the caller of
  `run()`. Calling `current_proposals()` anywhere else raises
  `ProposalContextError`.
- The scheduler closes a worker's handle when the worker settles. A task the
  worker left running that proposes after that gets `ProposalContextError`, so
  no proposal can be queued after `run()` returns.
- `propose_task(task, prerequisites=())` builds an `AddTaskProposal` and
  `propose_dependency(prerequisite_id, dependent_id)` builds an
  `AddDependencyProposal`. Both are frozen Pydantic models, joined as
  `MutationProposal`. `proposed_by` is stamped from the task id the scheduler
  bound, so a worker cannot propose on behalf of another task. Invalid input,
  such as an empty id, raises a Pydantic `ValidationError` in the worker and
  queues nothing.
- Each call only enqueues the proposal and returns it; the graph is unchanged
  when the call returns. The handle holds a queue reference and a task id, never
  the graph or the execution state. `Scheduler` has no public submit method.
- The scheduler drains the queue in submission order and applies each proposal
  through `add_task(spec, prerequisites=...)` or `add_dependency`, so proposals
  follow the same rules as direct mutations. A proposed task and the edges from
  its prerequisites are created in one step.
- A proposal can be rejected with `CycleError`, `TaskStartedError`,
  `DuplicateTaskError`, or `MissingTaskError`. A rejection changes nothing and
  is recorded in the run summary. It is never raised into the worker, and it
  never fails the proposer's task or the run. Later proposals are still applied.
- Proposals from a worker that raises are still applied, and that worker's
  task is marked `FAILED` as usual.

## Run summary

`run()` returns a `RunSummary` with sorted, disjoint `completed`, `failed`, and
`blocked` id tuples, the final `states` map, and `proposals`.

`proposals` is a tuple of `ProposalOutcome(proposal, applied, reason)` values,
one for each proposal processed during that `run()` call, in application order,
which is also submission order. `proposal` is the frozen model the worker got
back. `reason` is `None` when the proposal was applied; otherwise it is the
error class name and message, such as
`"TaskStartedError: dependent already started: A is RUNNING"`. A run with no
proposals has `proposals == ()`.

## Quick start

```python
import asyncio

from dagvora import Scheduler, TaskGraph, TaskSpec


class PrintExecutor:
    async def execute(self, spec: TaskSpec) -> None:
        print(spec.title)


graph = TaskGraph()
graph.add_task(TaskSpec(id="schema", title="Build schema"))
graph.add_task(TaskSpec(id="api", title="Build API"))
graph.add_dependency("schema", "api")

summary = asyncio.run(Scheduler(graph, PrintExecutor()).run())
print(summary.completed)
```

## Commands

```bash
uv sync
uv run ruff check .
uv run pytest
```

See [docs/PROJECT_CONTEXT.md](docs/PROJECT_CONTEXT.md) for the product boundary
and current roadmap.
