# Dagvora

An agentic software factory built around a runtime-mutable directed acyclic
graph (DAG). Dagvora schedules independent work concurrently and safely adds
new tasks or dependencies as workers discover them.

The current implementation starts with the validated task schema. The graph,
scheduler, runtime mutation, and LLM planner come next.

## Quick start

```bash
uv sync
uv run python -c "from dagvora import Task; print(Task(id='schema', title='Build schema'))"
```

See [docs/PROJECT_CONTEXT.md](docs/PROJECT_CONTEXT.md) for the product boundary
and current roadmap.
