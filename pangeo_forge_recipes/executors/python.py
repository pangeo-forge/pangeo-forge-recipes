from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Generator

from ..recipes.base import BaseRecipe
from .base import Pipeline, PipelineExecutor

logger = logging.getLogger(__name__)

GeneratorPipeline = Generator[Any, None, None]


class GeneratorPipelineExecutor(PipelineExecutor[GeneratorPipeline]):
    """An executor which returns a Generator.
    The Generator yeilds `function, args, kwargs`, which can be called step by step
    to iterate through the recipe.
    """

    @staticmethod
    def compile(pipeline: Pipeline):
        def generator_function():
            for stage in pipeline.stages:
                if stage.mappable is not None:
                    for m in stage.mappable:
                        yield stage.function, (m,), dict(config=pipeline.config)
                else:
                    yield stage.function, (), dict(config=pipeline.config)

        return generator_function()

    @staticmethod
    def execute(generator: GeneratorPipeline) -> None:
        for func, args, kwargs in generator:
            func(*args, **kwargs)


class FunctionPipelineExecutor(PipelineExecutor[Callable]):
    """A generator which returns a single callable python function with no
    arguments. Calling this function will run the whole recipe"""

    @staticmethod
    def compile(pipeline: Pipeline):
        def function():
            for stage in pipeline.stages:
                if stage.mappable is not None:
                    for m in stage.mappable:
                        stage.function(m, config=pipeline.config)
                else:
                    stage.function(config=pipeline.config)

        return function

    @staticmethod
    def execute(func: Callable) -> None:
        func()


@dataclass
class NamedManualStages:
    """An expressive convenience wrapper for the ``recipe.to_generator()`` manual execution mode.
    Introspect and execute sequential execution stages for the recipe instance by name.

    :param recipe: An instance of a Pangeo Forge recipe class.
    """

    recipe: BaseRecipe

    @property
    def _stages(self) -> list:
        return [func.__name__ for func, _, _ in self.recipe.to_generator()]

    @property
    def stage_names(self) -> tuple:
        """The unique stage names for the recipe instance. The order of the stages in this tuple
        is the sequential order in which they must be executed.
        """
        # Remove duplicates from a list while preserving order https://stackoverflow.com/a/480227
        seen: set = set()
        seen_add = seen.add
        return tuple(s for s in self._stages if not (s in seen or seen_add(s)))

    @property
    def metadata(self) -> dict:
        """A dictionary mapping each stage name to its place in the execution sequence
        (i.e., its `index`), and the number of function calls made by this stage (`ncalls`).
        """
        return {
            name: dict(index=i, ncalls=self._stages.count(name))
            for i, name in enumerate(self.stage_names)
        }

    def execute_stage(self, name: str, ncall_range: tuple[int] = (0,)) -> None:
        """Execute a recipe stage by name. For stages which include more than one function call,
        optionally specify the range of calls to be made.

        :param name: The stage name to execute. Allowed values can be introspected via the
        ``stage_names`` property of this class.
        :param ncall_range: A 1-tuple or 2-tuple of non-negative integers representing the
        interation start and stop values for execution of stages with more than one function call.
        Execution interations are 0-indexed and non-inclusive of the stop value. The number of
        function calls per stage can be introspected via the ``metadata`` property of this class.
        To execute all calls, keep the default ``ncall_range=(0,)``.
        """
        if (
            not 0 < len(ncall_range) <= 2
            or not all((lambda t: [isinstance(n, int) for n in t])(ncall_range))
            or not all((lambda t: [(n >= 0) for n in t])(ncall_range))
        ):
            raise ValueError(
                f"`{ncall_range = }` is not a 1-tuple or 2-tuple of non-negative integers."
            )

        _range = [n for n in ncall_range]
        # Add upper bound to execution range if `ncall_range` was passed as 1-tuple
        _range = _range if len(_range) == 2 else _range + [self.metadata[name]["ncalls"] + 1]

        logger.info(
            f"Executing `'{name}'`, "
            f"stage {self.metadata[name]['index'] + 1} of {len(self.stage_names)} stages."
        )
        ncalls = 0  # Not using `enumerate` b/c a stage can begin at any iteration of `to_generator`
        for function, args, kwargs in self.recipe.to_generator():
            if function.__name__ == name:
                if _range[0] <= ncalls < _range[1]:
                    logger.debug(f"Calling `{name}` function with `{args = }` and `{kwargs = }`.")
                    logger.info(
                        f"Call {ncalls + 1} of {self.metadata[name]['ncalls']} for this stage."
                    )
                    function(*args, **kwargs)
                ncalls += 1
                if ncalls == _range[1]:
                    break  # Saves loop from running through all iterations of `to_generator`
