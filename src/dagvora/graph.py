from collections import deque

from .exceptions import CycleError, DuplicateTaskError, MissingTaskError
from .models import TaskSpec


class TaskGraph:
    def __init__(self) -> None:
        self.tasks: dict[str, TaskSpec] = {}
        self.dependencies: dict[str, set[str]] = {}
        self.dependents: dict[str, set[str]] = {}

    def add_task(self, task: TaskSpec) -> None:
        if task.id in self.tasks:
            raise DuplicateTaskError(f"task id already exists: {task.id}")
        self.tasks[task.id] = task
        self.dependencies[task.id] = set()
        self.dependents[task.id] = set()

    def add_dependency(self, prerequisite_id: str, dependent_id: str) -> None:
        if prerequisite_id not in self.tasks:
            raise MissingTaskError(f"missing task: {prerequisite_id}")
        if dependent_id not in self.tasks:
            raise MissingTaskError(f"missing task: {dependent_id}")
        if prerequisite_id == dependent_id:
            # self dependency is a cycle failure
            raise CycleError(
                f"self dependency would create a cycle: {prerequisite_id}"
            )
        if prerequisite_id in self.dependencies[dependent_id]:
            return
        if self._is_reachable(dependent_id, prerequisite_id):
            raise CycleError(
                f"dependency would create a cycle: "
                f"{prerequisite_id} -> {dependent_id}"
            )
        self.dependencies[dependent_id].add(prerequisite_id)
        self.dependents[prerequisite_id].add(dependent_id)

    def topological_sort(self) -> list[str]:
        indegrees = {
            task_id: len(self.dependencies[task_id]) for task_id in self.tasks
        }
        ready = deque(
            task_id for task_id, indegree in indegrees.items() if indegree == 0
        )
        ordered: list[str] = []

        while ready:
            task_id = ready.popleft()
            ordered.append(task_id)
            for dependent_id in self.dependents[task_id]:
                indegrees[dependent_id] -= 1
                if indegrees[dependent_id] == 0:
                    ready.append(dependent_id)

        if len(ordered) < len(self.tasks):
            cycle_task_ids = [
                task_id for task_id, indegree in indegrees.items() if indegree > 0
            ]
            raise CycleError(
                "cycle detected involving task ids: "
                + ", ".join(cycle_task_ids)
            )
        return ordered

    def _is_reachable(self, start_id: str, target_id: str) -> bool:
        pending = [start_id]
        visited: set[str] = set()

        while pending:
            task_id = pending.pop()
            if task_id == target_id:
                return True
            if task_id in visited:
                continue
            visited.add(task_id)
            pending.extend(self.dependents[task_id] - visited)

        return False
