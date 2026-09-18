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

Runtime dependencies may only be added to `PENDING` or `READY` tasks. In Stage 4, workers will submit mutation proposals through a queue, and the orchestrator will validate and apply them.

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

Stages 1, 2, and 3 are complete.

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

Fifty-eight tests pass: fifteen graph and schema, nineteen execution state, twenty-four scheduler. Scheduler tests use `asyncio.Event` for ordering and run through `asyncio.run` from sync test functions; there is no sleep-based sequencing and no async test plugin.

The Stage 1 inconsistencies are resolved. `models.py` holds the schema, `graph.py` no longer defines a task type, and the README matches the code.

## Next milestone: Stage 4

Add the structured mutation proposal queue. Workers submit proposals for new tasks or dependencies; the orchestrator validates and applies them through `Scheduler.add_task` and `Scheduler.add_dependency`.

## Roadmap

- Stage 1: graph core, COMPLETE
- Stage 2: execution core, COMPLETE
- Stage 3: live graph mutation, COMPLETE
- Stage 4: structured mutation proposal queue, NEXT
- Stage 5: LLM planner
- Stage 6: isolated coding-agent workers

Later versions may add persistence, retries, checkpointing, cancellation, reversion, graph versioning, and distributed workers.