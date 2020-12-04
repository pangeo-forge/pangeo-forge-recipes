from functools import partial
from typing import Callable, Iterable

# from ..types import Pipeline, Stage, Task
from ..recipe import DatasetRecipe

Task = Callable[[], None]


class PythonExecutor:
    def prepare_plan(self, r: DatasetRecipe) -> Task:
        tasks = []
        tasks.append(r.prepare)
        for input in r.iter_inputs():
            tasks.append(partial(r.cache_input, input))
        for chunk in r.iter_chunks():
            tasks.append(partial(r.store_chunk, chunk))
        tasks.append(r.finalize)

        return partial(_execute_all, tasks)

    def execute_plan(self, plan: Task, **kwargs):
        plan()


def _execute_all(tasks: Iterable[Task]) -> None:
    for task in tasks:
        task()
