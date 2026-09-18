import pytest
from pydantic import ValidationError

from dagvora.exceptions import (
    CycleError,
    DuplicateTaskError,
    MissingTaskError,
)
from dagvora.graph import TaskGraph
from dagvora.models import TaskSpec


def assert_valid_order(graph: TaskGraph, order: list[str]) -> None:
    assert set(order) == set(graph.tasks)
    assert len(order) == len(graph.tasks)

    position = {task_id: index for index, task_id in enumerate(order)}

    for dependent_id, prerequisites in graph.dependencies.items():
        for prerequisite_id in prerequisites:
            assert position[prerequisite_id] < position[dependent_id]


def test_empty_graph_has_empty_topological_order() -> None:
    graph = TaskGraph()

    assert graph.topological_sort() == []


def test_single_task_has_exact_topological_order() -> None:
    graph = TaskGraph()
    task = TaskSpec(id="A", title="A")

    graph.add_task(task)

    assert graph.tasks["A"] is task
    assert graph.dependents["A"] == set()
    assert graph.topological_sort() == ["A"]


def test_chain_topological_order_is_valid() -> None:
    graph = TaskGraph()
    for task_id in ("A", "B", "C"):
        graph.add_task(TaskSpec(id=task_id, title=task_id))

    graph.add_dependency("A", "B")
    assert graph.dependencies["B"] == {"A"}
    assert graph.dependents["A"] == {"B"}

    graph.add_dependency("B", "C")
    assert graph.dependencies["C"] == {"B"}
    assert graph.dependents["B"] == {"C"}

    assert_valid_order(graph, graph.topological_sort())


def test_diamond_topological_order_is_valid() -> None:
    graph = TaskGraph()
    for task_id in ("A", "B", "C", "D"):
        graph.add_task(TaskSpec(id=task_id, title=task_id))

    graph.add_dependency("A", "B")
    assert graph.dependencies["B"] == {"A"}
    assert graph.dependents["A"] == {"B"}

    graph.add_dependency("A", "C")
    assert graph.dependencies["C"] == {"A"}
    assert graph.dependents["A"] == {"B", "C"}

    graph.add_dependency("B", "D")
    assert graph.dependencies["D"] == {"B"}
    assert graph.dependents["B"] == {"D"}

    graph.add_dependency("C", "D")
    assert graph.dependencies["D"] == {"B", "C"}
    assert graph.dependents["C"] == {"D"}

    assert_valid_order(graph, graph.topological_sort())


def test_disconnected_components_have_valid_topological_order() -> None:
    graph = TaskGraph()
    for task_id in ("A", "B", "C", "D"):
        graph.add_task(TaskSpec(id=task_id, title=task_id))

    graph.add_dependency("A", "B")
    assert graph.dependencies["B"] == {"A"}
    assert graph.dependents["A"] == {"B"}

    graph.add_dependency("C", "D")
    assert graph.dependencies["D"] == {"C"}
    assert graph.dependents["C"] == {"D"}

    assert_valid_order(graph, graph.topological_sort())


def test_self_dependency_is_rejected() -> None:
    graph = TaskGraph()
    graph.add_task(TaskSpec(id="A", title="A"))

    with pytest.raises(CycleError):
        graph.add_dependency("A", "A")

    assert graph.dependencies["A"] == set()
    assert graph.dependents["A"] == set()


def test_dependency_that_closes_cycle_is_rejected() -> None:
    graph = TaskGraph()
    for task_id in ("A", "B", "C"):
        graph.add_task(TaskSpec(id=task_id, title=task_id))

    graph.add_dependency("A", "B")
    assert graph.dependencies["B"] == {"A"}
    assert graph.dependents["A"] == {"B"}

    graph.add_dependency("B", "C")
    assert graph.dependencies["C"] == {"B"}
    assert graph.dependents["B"] == {"C"}

    with pytest.raises(CycleError):
        graph.add_dependency("C", "A")

    assert graph.dependencies["A"] == set()
    assert graph.dependents["C"] == set()


def test_duplicate_task_id_is_rejected() -> None:
    graph = TaskGraph()
    original_task = TaskSpec(id="A", title="A")
    graph.add_task(original_task)

    with pytest.raises(DuplicateTaskError):
        graph.add_task(TaskSpec(id="A", title="A"))

    assert graph.tasks["A"] is original_task
    assert graph.dependents["A"] == set()


def test_dependency_with_missing_task_is_rejected() -> None:
    graph = TaskGraph()
    graph.add_task(TaskSpec(id="A", title="A"))

    with pytest.raises(MissingTaskError):
        graph.add_dependency("A", "missing")

    assert graph.dependencies["A"] == set()
    assert graph.dependents["A"] == set()


def test_duplicate_dependency_is_idempotent() -> None:
    graph = TaskGraph()
    graph.add_task(TaskSpec(id="A", title="A"))
    graph.add_task(TaskSpec(id="B", title="B"))

    graph.add_dependency("A", "B")
    assert graph.dependencies["B"] == {"A"}
    assert graph.dependents["A"] == {"B"}

    dependencies_before = set(graph.dependencies["B"])
    dependents_before = set(graph.dependents["A"])

    graph.add_dependency("A", "B")

    assert graph.dependencies["B"] == dependencies_before
    assert graph.dependents["A"] == dependents_before
    assert len(graph.dependencies["B"]) == 1
    assert len(graph.dependents["A"]) == 1


def test_rejected_dependency_leaves_graph_unchanged() -> None:
    graph = TaskGraph()
    for task_id in ("A", "B", "C"):
        graph.add_task(TaskSpec(id=task_id, title=task_id))

    graph.add_dependency("A", "B")
    assert graph.dependencies["B"] == {"A"}
    assert graph.dependents["A"] == {"B"}

    graph.add_dependency("B", "C")
    assert graph.dependencies["C"] == {"B"}
    assert graph.dependents["B"] == {"C"}

    before_dependencies = {
        task_id: set(prerequisites)
        for task_id, prerequisites in graph.dependencies.items()
    }
    before_dependents = {
        task_id: set(dependent_ids)
        for task_id, dependent_ids in graph.dependents.items()
    }

    with pytest.raises(CycleError):
        graph.add_dependency("C", "A")

    after_dependencies = {
        task_id: set(prerequisites)
        for task_id, prerequisites in graph.dependencies.items()
    }
    after_dependents = {
        task_id: set(dependent_ids)
        for task_id, dependent_ids in graph.dependents.items()
    }

    assert after_dependencies == before_dependencies
    assert after_dependents == before_dependents


def test_graph_remains_usable_after_rejected_dependency() -> None:
    graph = TaskGraph()
    for task_id in ("A", "B", "C"):
        graph.add_task(TaskSpec(id=task_id, title=task_id))

    graph.add_dependency("A", "B")
    assert graph.dependencies["B"] == {"A"}
    assert graph.dependents["A"] == {"B"}

    with pytest.raises(CycleError):
        graph.add_dependency("B", "A")

    graph.add_dependency("B", "C")
    assert graph.dependencies["C"] == {"B"}
    assert graph.dependents["B"] == {"C"}

    assert_valid_order(graph, graph.topological_sort())


def test_task_spec_rejects_empty_id() -> None:
    with pytest.raises(ValidationError):
        TaskSpec(id="", title="A")


def test_task_spec_rejects_empty_title() -> None:
    with pytest.raises(ValidationError):
        TaskSpec(id="A", title="")


def test_task_spec_description_defaults_to_none() -> None:
    task = TaskSpec(id="A", title="A")

    assert task.description is None
