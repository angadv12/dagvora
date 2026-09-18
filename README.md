# Dagvora

An agentic software factory built around a runtime-mutable directed acyclic
graph (DAG). Dagvora schedules independent work concurrently and safely adds
new tasks or dependencies as workers discover them.

The current implementation covers the static graph, the execution core, and
live graph mutation. The mutation proposal protocol and the LLM planner come
next.

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

1. Promote every `PENDING` task whose prerequisites are all `COMPLETED` to
   `READY`.
2. Move each `READY` task to `RUNNING` and start it on the executor, tracking
   the in-flight `asyncio.Task` objects explicitly.
3. Stop when nothing is running.
4. Otherwise wait with `asyncio.wait(..., return_when=asyncio.FIRST_COMPLETED)`
   on the running tasks and a wakeup future, and settle each finished task as
   `COMPLETED` or `FAILED`.

Because the loop reacts to the first completion rather than the whole batch,
a task unlocked by a fast prerequisite starts immediately while slower siblings
are still running.

A worker exception marks only that task `FAILED`; it is not re-raised and it
never fails other tasks. Independent branches keep running to completion. The
dependents of a failed task simply never satisfy readiness, so they finish the
run `PENDING` and are reported as `blocked`.

## Runtime mutation

While `run()` is active, the orchestrator changes the graph through the
scheduler, never through `TaskGraph` or `ExecutionState` directly:

```python
scheduler.add_task(TaskSpec(id="migrate", title="Write migration"))
scheduler.add_dependency("schema", "migrate")
```

- `add_task` registers the spec in both `TaskGraph` and `ExecutionState` as
  `PENDING`. A duplicate id raises `DuplicateTaskError`.
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

Add a task and its prerequisites without awaiting in between. Otherwise the task
may start first, and the late dependency is rejected. A run ends when nothing is
running; mutations made after that are picked up by the next `run()`.

## Executors

An executor is any object with an async `execute` method:

```python
class Executor(Protocol):
    async def execute(self, spec: TaskSpec) -> None: ...
```

Returning normally completes the task; raising fails it.

## Run summary

`run()` returns a `RunSummary` with sorted, disjoint `completed`, `failed`, and
`blocked` id tuples plus the final `states` map.

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
