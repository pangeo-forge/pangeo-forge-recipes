import itertools
from dataclasses import dataclass
from typing import Optional, Iterable, Tuple, Any, List

import apache_beam as beam

from .base import (
    Pipeline,
    PipelineExecutor,
    Config,
    Stage,
    NoArgumentStageFunction,
    SingleArgumentStageFunction
)


def _exec_no_arg_stage(
    last: int, *, current: int, function: NoArgumentStageFunction, config: Config
) -> int:
    """Execute a NoArgumentStageFunction, ensuring execution order."""
    assert (last + 1) == current, f'stages are executing out of order! On step {current!r}.'

    function(config=config)

    return current


def _no_op(arg, config=None) -> None:
    pass


@dataclass()
class _SingleArgumentStage(beam.PTransform):
    """Execute mappable stage in parallel."""

    step: int
    stage: Stage
    config: Config

    @staticmethod
    def prepare_stage(last: int, *, current: int, mappable: Iterable) -> Iterable[Tuple[int, Any]]:
        """Propagate current stage to Mappables for parallel execution."""
        assert (last + 1) == current, f'stages are executing out of order! On step {current!r}.'
        return zip(itertools.repeat(current), mappable)

    @staticmethod
    def exec_stage(
        last: int, arg,
        current: int = -1, function: SingleArgumentStageFunction = _no_op, config: Optional[Config] = None
    ) -> int:
        """Execute stage function."""
        assert last == current, f'stages are executing out of order! On step {current!r}.'

        function(arg, config=config)

        return current

    @staticmethod
    def post_validate(last: List[int], *, current: int) -> int:
        """Propagate step number for downstream stage validation."""
        assert all([it == current for it in last]), f'stages are executing out of order! On step {current!r}.'
        return current

    def expand(self, pcoll):
        return (
                pcoll
                | "Prepare" >> beam.FlatMap(self.prepare_stage, current=self.step, mappable=self.stage.mappable)
                | "Execute" >> beam.MapTuple(self.exec_stage, current=self.step, function=self.stage.function,
                                             config=self.config)
                | beam.combiners.ToList()
                | "Validate" >> beam.Map(self.post_validate, current=self.step)
        )


class BeamPipelineExecutor(PipelineExecutor[beam.PTransform]):
    @staticmethod
    def compile(pipeline: Pipeline) -> beam.PTransform:
        pcoll = beam.Create([-1])
        for step, stage in enumerate(pipeline.stages):
            if stage.mappable is not None:
                pcoll |= stage.name >> _SingleArgumentStage(step, stage, pipeline.config)
            else:
                pcoll |= stage.name >> beam.Map(_exec_no_arg_stage,
                                                current=step,
                                                function=stage.function,
                                                config=pipeline.config)

        return pcoll

    @staticmethod
    def execute(plan: beam.PTransform, *args, **kwargs):
        """Execute a plan. All args and kwargs are pass to a `apache_beam.Pipeline`."""
        with beam.Pipeline(*args, **kwargs) as p:
            p | plan
