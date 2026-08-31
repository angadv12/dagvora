import pytest

from dagvora.exceptions import (
    CycleError,
    DuplicateTaskError,
    MissingTaskError,
)
from dagvora.graph import Task, TaskGraph


def assert_valid_order(graph: TaskGraph, order: list[str]) -> None:
    assert set(order) == set(graph.tasks)
    assert len(order) == len(graph.tasks)

    position = {task_id: index for index, task_id in enumerate(order)}

    for dependent_id, task in graph.tasks.items():
        for prerequisite_id in task.dependencies:
            assert position[prerequisite_id] < position[dependent_id]


def test_empty_graph_has_empty_topological_order() -> None:
    graph = TaskGraph()

    assert [] == graph.topological_sort()  # noqa: SIM300


def test_single_task_has_exact_topological_order() -> None:
    graph = TaskGraph()
    task = Task("A")

    graph.add_task(task)

    assert graph.tasks["A"] is task
    assert graph.dependents["A"] == set()
    assert graph.topological_sort() == ["A"]


def test_chain_topological_order_is_valid() -> None:
    graph = TaskGraph()
    for task_id in ("A", "B", "C"):
        graph.add_task(Task(task_id))

    graph.add_dependency("A", "B")
    assert graph.tasks["B"].dependencies == {"A"}
    assert graph.dependents["A"] == {"B"}

    graph.add_dependency("B", "C")
    assert graph.tasks["C"].dependencies == {"B"}
    assert graph.dependents["B"] == {"C"}

    assert_valid_order(graph, graph.topological_sort())


def test_diamond_topological_order_is_valid() -> None:
    graph = TaskGraph()
    for task_id in ("A", "B", "C", "D"):
        graph.add_task(Task(task_id))

    graph.add_dependency("A", "B")
    assert graph.tasks["B"].dependencies == {"A"}
    assert graph.dependents["A"] == {"B"}

    graph.add_dependency("A", "C")
    assert graph.tasks["C"].dependencies == {"A"}
    assert graph.dependents["A"] == {"B", "C"}

    graph.add_dependency("B", "D")
    assert graph.tasks["D"].dependencies == {"B"}
    assert graph.dependents["B"] == {"D"}

    graph.add_dependency("C", "D")
    assert graph.tasks["D"].dependencies == {"B", "C"}
    assert graph.dependents["C"] == {"D"}

    assert_valid_order(graph, graph.topological_sort())


def test_disconnected_components_have_valid_topological_order() -> None:
    graph = TaskGraph()
    for task_id in ("A", "B", "C", "D"):
        graph.add_task(Task(task_id))

    graph.add_dependency("A", "B")
    assert graph.tasks["B"].dependencies == {"A"}
    assert graph.dependents["A"] == {"B"}

    graph.add_dependency("C", "D")
    assert graph.tasks["D"].dependencies == {"C"}
    assert graph.dependents["C"] == {"D"}

    assert_valid_order(graph, graph.topological_sort())


def test_self_dependency_is_rejected() -> None:
    graph = TaskGraph()
    graph.add_task(Task("A"))

    with pytest.raises(CycleError):
        graph.add_dependency("A", "A")

    assert graph.tasks["A"].dependencies == set()
    assert graph.dependents["A"] == set()


def test_dependency_that_closes_cycle_is_rejected() -> None:
    graph = TaskGraph()
    for task_id in ("A", "B", "C"):
        graph.add_task(Task(task_id))

    graph.add_dependency("A", "B")
    assert graph.tasks["B"].dependencies == {"A"}
    assert graph.dependents["A"] == {"B"}

    graph.add_dependency("B", "C")
    assert graph.tasks["C"].dependencies == {"B"}
    assert graph.dependents["B"] == {"C"}

    with pytest.raises(CycleError):
        graph.add_dependency("C", "A")

    assert graph.tasks["A"].dependencies == set()
    assert graph.dependents["C"] == set()


def test_duplicate_task_id_is_rejected() -> None:
    graph = TaskGraph()
    original_task = Task("A")
    graph.add_task(original_task)

    with pytest.raises(DuplicateTaskError):
        graph.add_task(Task("A"))

    assert graph.tasks["A"] is original_task
    assert graph.dependents["A"] == set()


def test_dependency_with_missing_task_is_rejected() -> None:
    graph = TaskGraph()
    graph.add_task(Task("A"))

    with pytest.raises(MissingTaskError):
        graph.add_dependency("A", "missing")

    assert graph.tasks["A"].dependencies == set()
    assert graph.dependents["A"] == set()


def test_duplicate_dependency_is_idempotent() -> None:
    graph = TaskGraph()
    graph.add_task(Task("A"))
    graph.add_task(Task("B"))

    graph.add_dependency("A", "B")
    assert graph.tasks["B"].dependencies == {"A"}
    assert graph.dependents["A"] == {"B"}

    dependencies_before = set(graph.tasks["B"].dependencies)
    dependents_before = set(graph.dependents["A"])

    graph.add_dependency("A", "B")

    assert graph.tasks["B"].dependencies == dependencies_before
    assert graph.dependents["A"] == dependents_before
    assert len(graph.tasks["B"].dependencies) == 1
    assert len(graph.dependents["A"]) == 1


def test_rejected_dependency_leaves_graph_unchanged() -> None:
    graph = TaskGraph()
    for task_id in ("A", "B", "C"):
        graph.add_task(Task(task_id))

    graph.add_dependency("A", "B")
    assert graph.tasks["B"].dependencies == {"A"}
    assert graph.dependents["A"] == {"B"}

    graph.add_dependency("B", "C")
    assert graph.tasks["C"].dependencies == {"B"}
    assert graph.dependents["B"] == {"C"}

    before_tasks = {
        task_id: set(task.dependencies)
        for task_id, task in graph.tasks.items()
    }
    before_dependents = {
        task_id: set(dependent_ids)
        for task_id, dependent_ids in graph.dependents.items()
    }

    with pytest.raises(CycleError):
        graph.add_dependency("C", "A")

    after_tasks = {
        task_id: set(task.dependencies)
        for task_id, task in graph.tasks.items()
    }
    after_dependents = {
        task_id: set(dependent_ids)
        for task_id, dependent_ids in graph.dependents.items()
    }

    assert after_tasks == before_tasks
    assert after_dependents == before_dependents


def test_graph_remains_usable_after_rejected_dependency() -> None:
    graph = TaskGraph()
    for task_id in ("A", "B", "C"):
        graph.add_task(Task(task_id))

    graph.add_dependency("A", "B")
    assert graph.tasks["B"].dependencies == {"A"}
    assert graph.dependents["A"] == {"B"}

    with pytest.raises(CycleError):
        graph.add_dependency("B", "A")

    graph.add_dependency("B", "C")
    assert graph.tasks["C"].dependencies == {"B"}
    assert graph.dependents["B"] == {"C"}

    assert_valid_order(graph, graph.topological_sort())
