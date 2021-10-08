from typing import Callable

from .base import Pipeline, PipelineExecutor


class FunctionPipelineExecutor(PipelineExecutor[Callable]):

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
