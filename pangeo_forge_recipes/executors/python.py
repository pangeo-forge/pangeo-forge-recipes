from __future__ import annotations

import logging
from collections import OrderedDict
from typing import Any, Callable, Dict, Generator, Tuple

from .base import Pipeline, PipelineExecutor

logger = logging.getLogger(__name__)

GeneratorPipeline = Generator[Any, None, None]
ManualPipeline = Tuple[Dict, Callable]


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


class ManualPipelineExecutor(PipelineExecutor[ManualPipeline]):
    """An executor which returns a 2-tuple containing an `OrderedDict` mapping sequential stage
    names to the number of times the cooresponding stage function is called, and a callable
    which can be used to execute stages by name. The callable optionally takes an ``ncall_range``
    argument which can be used to optionally specify a subsetted range of calls to be made.
    """

    @staticmethod
    def compile(pipeline: Pipeline):
        stages = pipeline.stages
        ncalls = OrderedDict(
            {s.name: (len(s.mappable) if s.mappable else 1) for s in stages}  # type: ignore
        )
        indices = {name: i for i, name in enumerate(tuple(ncalls))}

        def execute_stage(name: str, ncall_range: tuple[int] = (0,)) -> None:
            """Execute a recipe stage by name. For stages which include more than one function call,
            optionally specify the range of calls to be made.

            :param name: The stage name to execute.
            :param ncall_range: A 1-tuple or 2-tuple of non-negative integers representing the
            interation start and stop values for execution of stages with more than one function
            call. Execution interations are 0-indexed and non-inclusive of the stop value. To
            execute all calls, keep the default ``ncall_range=(0,)``.
            """
            if (
                not 0 < len(ncall_range) <= 2
                or not all((lambda t: [isinstance(n, int) for n in t])(ncall_range))
                or not all((lambda t: [(n >= 0) for n in t])(ncall_range))
            ):
                raise ValueError(
                    f"`{ncall_range = }` is not a 1-tuple or 2-tuple of non-negative integers."
                )

            for s in stages:
                if s.name == name:
                    stage = s

            _range = [n for n in ncall_range]
            # Add upper bound to execution range if `ncall_range` was passed as 1-tuple
            _range = _range if len(_range) == 2 else _range + [ncalls[name] + 1]

            logger.info(
                f"Executing `'{name}'`, stage {indices[name] + 1} of {len(list(indices))} stages."
            )
            kw = dict(config=pipeline.config)
            if stage.mappable is not None:
                for i, m in enumerate(stage.mappable):
                    if _range[0] <= i < _range[1]:
                        logger.debug(f"Calling `{name}` with `args=({m},)` and `kwargs={kw}`.")
                        logger.info(f"Call {i + 1} of {ncalls[name]} for this stage.")
                        stage.function(m, **kw)  # type: ignore
                    if i == _range[1]:
                        break  # Possibly useful for stages with very large mappables
            else:
                logger.debug(f"Calling `{name}` with `args = ()` and `kwargs = {kw}`.")
                logger.info("Call 1 of 1 for this stage.")
                stage.function(**kw)  # type: ignore

        return ncalls, execute_stage

    @staticmethod
    def execute(manual_pipeline: ManualPipeline):
        ncalls, execute_stage = manual_pipeline
        for stage_name in ncalls.keys():
            execute_stage(stage_name)
