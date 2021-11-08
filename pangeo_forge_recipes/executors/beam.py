import itertools
from dataclasses import dataclass
from typing import Any, Iterable, List, Tuple, cast

import apache_beam as beam

from .base import Config, NoArgumentStageFunction, Pipeline, PipelineExecutor, Stage


def _no_arg_stage(last: int, *, current: int, func: NoArgumentStageFunction, config: Config) -> int:
    """Execute a NoArgumentStageFunction, ensuring execution order."""
    assert (last + 1) == current, f"stages are executing out of order! On step {current!r}."

    func(config=config)

    return current


def _no_op(arg, config=None) -> None:
    pass


@dataclass()
class _SingleArgumentStage(beam.PTransform):
    """Execute mappable stage in parallel."""

    step: int
    stage: Stage
    config: Config

    def prepare_stage(self, last: int) -> Iterable[Tuple[int, Any]]:
        """Propagate current stage to Mappables for parallel execution."""
        assert (last + 1) == self.step, f"stages are executing out of order! On step {self.step!r}."
        return zip(itertools.repeat(self.step), cast(Iterable, self.stage.mappable))

    def exec_stage(self, last: int, arg: Any) -> int:
        """Execute stage function."""
        assert last == self.step, f"stages are executing out of order! On step {self.step!r}."

        self.stage.function(arg, config=self.config)  # type: ignore

        return self.step

    def post_validate(self, last: List[int]) -> int:
        """Propagate step number for downstream stage validation."""
        in_current_step = all((it == self.step for it in last))
        assert in_current_step, f"stages are executing out of order! On step {self.step!r}."

        return self.step

    def expand(self, pcoll):
        return (
            pcoll
            | "Prepare" >> beam.FlatMap(self.prepare_stage)
            | "Execute" >> beam.MapTuple(self.exec_stage)
            | beam.combiners.ToList()
            | "Validate" >> beam.Map(self.post_validate)
        )


class BeamPipelineExecutor(PipelineExecutor[beam.PTransform]):
    @staticmethod
    def compile(pipeline: Pipeline) -> beam.PTransform:
        pcoll = beam.Create([-1])
        for step, stage in enumerate(pipeline.stages):
            if stage.mappable is not None:
                pcoll |= stage.name >> _SingleArgumentStage(step, stage, pipeline.config)
            else:
                pcoll |= stage.name >> beam.Map(
                    _no_arg_stage, current=step, func=stage.function, config=pipeline.config
                )

        return pcoll

    @staticmethod
    def execute(plan: beam.PTransform, *args, **kwargs):
        """Execute a plan. All args and kwargs are passed to a `apache_beam.Pipeline`."""
        with beam.Pipeline(*args, **kwargs) as p:
            p | plan
