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

Stage 1 and Stage 2 are complete.

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

Forty-two tests pass: fifteen graph and schema, seventeen execution state, ten scheduler. Scheduler tests use `asyncio.Event` for ordering and run through `asyncio.run` from sync test functions; there is no sleep-based sequencing and no async test plugin.

The Stage 1 inconsistencies are resolved. `models.py` holds the schema, `graph.py` no longer defines a task type, and the README matches the code.

## Next milestone: Stage 3

Implement live graph mutation:

1. Insert tasks while the scheduler is running
2. Insert dependencies while the scheduler is running
3. Enforce that a runtime dependency may only be added to a `PENDING` or `READY` dependent
4. Reuse the existing reachability check so a runtime edge cannot close a cycle
5. Keep the orchestrator the single writer for graph and execution state
6. Deterministic tests for mid-run insertion, rejected mutation, and unchanged running work

Do not implement the proposal queue in this milestone.

## Roadmap

- Stage 1: graph core, COMPLETE
- Stage 2: execution core, COMPLETE
- Stage 3: live graph mutation, NEXT
- Stage 4: structured mutation proposal queue
- Stage 5: LLM planner
- Stage 6: isolated coding-agent workers

Later versions may add persistence, retries, checkpointing, cancellation, reversion, graph versioning, and distributed workers.