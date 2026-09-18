# Dagvora project context

## Purpose

Dagvora is an agent runtime built around a task DAG that can change while work is running.

Workers may discover missing prerequisites and propose new tasks or dependencies. Only the orchestrator may validate and apply those changes.

The central V1 goal is safe runtime DAG mutation without cycles, changes to already-started work, or concurrent workers corrupting shared state.

## Core design

Dagvora separates three concepts:

```text
TaskSpec = what a task is
TaskGraph = how tasks depend on each other
ExecutionState = what is happening to each task
```

Workers execute tasks and submit results or mutation proposals. They never directly change the graph or execution state.

The orchestrator is the single writer for mutable runtime state.

## V1 behavior

Task states:

```text
PENDING
READY
RUNNING
COMPLETED
FAILED
```

A task is ready when it is `PENDING` and every dependency is `COMPLETED`.

Ready tasks run concurrently through an async scheduler.

For an edge `A -> B`, `A` is the prerequisite and `B` depends on `A`:

```python
graph.add_dependency("A", "B")
```

Before adding `A -> B`, search for an existing path from `B` to `A`. If one exists, reject the edge because it would create a cycle.

Runtime dependencies may only be added to `PENDING` or `READY` tasks. Workers submit mutation proposals through a queue, and the orchestrator validates and applies them.

If one task fails:

- Mark it `FAILED`
- Continue independent branches
- Leave its dependents `PENDING`
- Report remaining pending tasks as blocked in the final run summary

V1 does not retry, checkpoint, cancel, revert, or restart work.

## Technical choices

- Python 3.11+
- Pydantic for externally produced structured data such as `TaskSpec`
- Plain Python for internal runtime bookkeeping
- In-memory state for V1
- Kahn's algorithm for topological sorting
- DFS or BFS reachability for mutation-time cycle checks
- `asyncio.wait(..., return_when=asyncio.FIRST_COMPLETED)` for scheduling
- Single-writer orchestrator to avoid concurrent graph mutation

## Current state

Stages 1, 2, 3, and 4 are complete.

Stage 1, graph core:

- Task and dependency storage
- Bidirectional edge tracking
- Duplicate and missing-task validation
- Cycle-safe dependency insertion
- Atomic rejection of invalid edges
- Kahn topological sorting

Stage 2, execution core:

- `TaskSpec` in `src/dagvora/models.py` is a frozen Pydantic model with `id`, `title`, and optional `description`
- `TaskGraph` in `src/dagvora/graph.py` stores `TaskSpec` values and owns topology in `graph.dependencies` and `graph.dependents`; the `Task` dataclass is gone
- `ExecutionState` in `src/dagvora/state.py` is plain Python and owns only the id to `TaskState` map, `LEGAL_TRANSITIONS`, and an atomic `transition`
- An illegal transition raises `InvalidTransitionError` and leaves the state unchanged
- `Executor` and `Scheduler` in `src/dagvora/scheduler.py` run ready work concurrently, track in-flight `asyncio.Task` objects explicitly, and settle each completion as `COMPLETED` or `FAILED`
- A worker exception is never re-raised and never fails another task
- `RunSummary` carries sorted, disjoint `completed`, `failed`, and `blocked` id tuples plus the final `states` map

Stage 3, live graph mutation:

- `Scheduler.add_task` registers a new `TaskSpec` in both `TaskGraph` and `ExecutionState`; it checks the graph before registering state, so a duplicate id changes neither store
- `Scheduler.add_dependency` rejects a `RUNNING`, `COMPLETED`, or `FAILED` dependent with `TaskStartedError` before touching the graph, then delegates to `TaskGraph.add_dependency` for missing-task and cycle checks
- A `READY` dependent that gains an unfinished prerequisite returns to `PENDING`; `READY -> PENDING` is the one transition added to `LEGAL_TRANSITIONS`
- `run()` waits on its running tasks and a wakeup future that `add_task` resolves, so inserted work starts without waiting for a running task to finish
- Mutations are synchronous calls on the event loop, so the scheduler stays the single writer and never observes a partial mutation

Stage 4, structured mutation proposal queue:

- `src/dagvora/proposals.py` holds `AddTaskProposal` and `AddDependencyProposal`, frozen Pydantic models joined as `MutationProposal`; each records `proposed_by`, the id of the task that proposed it
- `ProposalQueue` holds proposals only, in submission order, and calls the scheduler's wakeup on every submit
- `ProposalHandle` holds a queue reference and a task id, never the graph or the state; `propose_task` and `propose_dependency` stamp `proposed_by` with that id, enqueue the proposal, and return it
- `Scheduler.run` starts each worker through a private `_execute` coroutine that binds the worker's handle in a `ContextVar` inside its own `asyncio.Task`; `current_proposals()` returns that handle and raises `ProposalContextError` outside a scheduled worker, and `Executor.execute(spec)` keeps its signature
- `_execute` closes the handle when its worker settles, so a task the worker left running cannot propose afterwards and nothing is queued after `run()` returns
- `Scheduler.add_task` takes keyword-only `prerequisites`; it validates the id and every prerequisite before writing, then registers the task and all its edges in one synchronous step, so a rejection is all-or-nothing and the task never exists without its edges
- `run()` drains the queue at the top of every iteration, before readiness promotion, and applies each proposal through `add_task` or `add_dependency`; a settled worker's proposals land before the readiness check that follows, and the run cannot end with proposals queued
- A submit resolves the run loop's wakeup future, so proposals from running workers are applied without waiting for any worker to finish
- A rejected proposal (`CycleError`, `TaskStartedError`, `DuplicateTaskError`, `MissingTaskError`) changes nothing and is recorded; it is never raised into the worker and never fails the proposer's task or the run
- Proposals from a worker that raises are still applied, and that worker's task is `FAILED` as usual
- `RunSummary.proposals` is a tuple of `ProposalOutcome(proposal, applied, reason)` in application order; `reason` is `None` when applied and the error class name and message when rejected

One hundred five tests pass: fifteen graph and schema, nineteen execution state, thirty-one scheduler, twenty proposal queue and handle, and twenty scheduler proposal drain. Scheduler and proposal tests use `asyncio.Event` for ordering and run through `asyncio.run` from sync test functions; there is no sleep-based sequencing and no async test plugin.

The Stage 1 inconsistencies are resolved. `models.py` holds the schema, `graph.py` no longer defines a task type, and the README matches the code.

## Next milestone: Stage 5

Add the LLM planner. Its output reaches the graph only as mutation proposals that the scheduler validates and applies.

## Roadmap

- Stage 1: graph core, COMPLETE
- Stage 2: execution core, COMPLETE
- Stage 3: live graph mutation, COMPLETE
- Stage 4: structured mutation proposal queue, COMPLETE
- Stage 5: LLM planner, NEXT
- Stage 6: isolated coding-agent workers

Later versions may add persistence, retries, checkpointing, cancellation, reversion, graph versioning, and distributed workers.