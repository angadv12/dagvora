# Dagvora project context

## Problem

Most agent planners create a fixed task graph before execution. Real workers
discover missing prerequisites after work begins. Dagvora is a small agent
runtime whose orchestrator can accept those discoveries and safely mutate the
task DAG without creating cycles, invalidating active work, or allowing
concurrent workers to corrupt shared state.

Example: an OAuth worker discovers that the session model needs a migration.
The orchestrator can add `session_migration` after `db_schema` and make OAuth
and API work depend on it, provided those downstream tasks have not started.

Workers may propose tasks and dependencies, but only the orchestrator owns and
mutates graph state.

## V1 behavior

1. A task is `PENDING`, `READY`, `RUNNING`, `COMPLETED`, or `FAILED`.
2. A task is ready when every prerequisite is complete.
3. Ready tasks run concurrently through an async scheduler.
4. New tasks may be inserted while the scheduler is running.
5. A dependency may be added only when its dependent is pending or ready.
6. Before adding `u -> v`, the graph searches for an existing path `v -> u`.
   If one exists, the mutation is rejected because it would create a cycle.

V1 deliberately does not cancel, checkpoint, revert, or restart running tasks.
It also has no persistence, distributed workers, graph versioning, retry policy,
or LLM integration.

## Delivery stages

- Stage 1: static task graph with task storage, safe dependency insertion, Kahn
  topological sorting, cycle rejection, and tests.
- Stage 2: execution core with task readiness, state transitions, and async
  scheduling.
- Stage 3: runtime mutation to insert tasks and dependencies while the scheduler
  runs, under the V1 policy.
- Stage 4: mutation protocol where workers submit structured proposals to a queue;
  the orchestrator serializes and applies them.
- Stage 5: LLM planner to convert a request such as “Add JWT authentication” into
  structured tasks and dependencies.
- Stage 6: workers that replace fake callables with isolated coding agents.

## Current state as of 2026-08-27

The validated task schema is implemented. The current task is the in-memory
`TaskGraph` with `add_task`, `add_dependency`, and `topological_sort`, plus its
tests. Readiness, scheduling, runtime mutation during execution, and worker
integration remain planned but unbuilt.

For graph edges, `add_dependency("A", "B")` means `A -> B`: task `B` depends
on task `A`. Incoming dependencies and outgoing dependents must remain
synchronized, and rejected mutations must leave the graph unchanged.

## Technical choices

- Python 3.11+; use `asyncio` when orchestration begins.
- Pydantic models for validation now and JSON Schema generation for later LLM
  structured output.
- In-memory graph storage for the first three stages.
- Kahn’s algorithm for full-graph validation and topological sorting:
  `O(V + E)` time.
- DFS or BFS reachability for mutation-time cycle checks: before adding
  `u -> v`, search outward from `v` for `u`. `O(V + E)` worst-case time is
  acceptable for the initial scale.
- Three-state DFS for reporting an explicit cycle path is deferred until that
  diagnostic is needed.
