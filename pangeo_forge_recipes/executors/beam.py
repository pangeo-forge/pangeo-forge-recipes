from typing import Optional, Iterable

import apache_beam as beam

from .base import Pipeline, PipelineExecutor, Config, StageFunction


def exec_ordered(last: int,
                 *,
                 current: int,
                 function: StageFunction,
                 config: Config,
                 mappable: Optional[Iterable]) -> int:
    """Execute a Pipeline Stage, ensuring execution order."""
    assert (last + 1) == current, 'stages are executing out of order!'

    if mappable is not None:
        for m in mappable:
            function(m, config=config)
    else:
        function(config=config)

    return current


class BeamPipelineExecutor(PipelineExecutor[beam.PTransform]):
    @staticmethod
    def compile(pipeline: Pipeline) -> beam.PTransform:
        pcoll = beam.Create([-1])
        for step, stage in enumerate(pipeline.stages):
            pcoll |= stage.name >> beam.Map(exec_ordered,
                                            current=step,
                                            function=stage.function,
                                            mappable=stage.mappable,
                                            config=pipeline.config)

        return pcoll

    @staticmethod
    def execute(plan: beam.PTransform, *args, **kwargs):
        with beam.Pipeline(*args, **kwargs) as p:
            p | plan
