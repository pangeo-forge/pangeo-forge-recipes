from typing import Any, Dict, List, Optional  # noqa: F401

from prefect import Flow, task, unmapped

from .base import Pipeline, PipelineExecutor, StageAnnotations, StageAnnotationType
from .dask import DaskPipelineExecutor


def annotations_to_task_kwargs(annotations: Optional[StageAnnotations]) -> Dict[str, Any]:
    if annotations is None:
        return {}
    task_kwargs = {}
    for ann_type in annotations:
        if ann_type == StageAnnotationType.RETRIES:
            task_kwargs["max_retries"] = annotations[ann_type]
        if ann_type == StageAnnotationType.CONCURRENCY:
            raise ValueError("Haven't figured out concurrency yet.")
    return task_kwargs


class PrefectPipelineExecutor(PipelineExecutor[Flow]):
    @staticmethod
    def compile(pipeline: Pipeline):

        with Flow("pangeo-forge-recipe") as flow:
            upstream_tasks = []  # type: List[task]
            for stage in pipeline.stages:
                task_kwargs = annotations_to_task_kwargs(stage.annotations)
                stage_task = task(stage.function, name=stage.name, **task_kwargs)
                if stage.mappable is not None:
                    stage_task_called = stage_task.map(
                        stage.mappable,
                        config=unmapped(pipeline.config),
                        upstream_tasks=[unmapped(t) for t in upstream_tasks],
                    )
                else:
                    stage_task_called = stage_task(
                        config=pipeline.config, upstream_tasks=upstream_tasks
                    )
                upstream_tasks = [stage_task_called]

        return flow

    @staticmethod
    def execute(flow: Flow):
        return flow.run()


@task
def run_pipeline_with_dask(pipeline):
    delayed = DaskPipelineExecutor.compile(pipeline)
    # how do we make sure this uses the right dask cluster?
    delayed.compute()


class PrefectDaskWrapperExecutor(PipelineExecutor[Flow]):
    @staticmethod
    def compile(pipeline: Pipeline):
        with Flow("pangeo-forge-recipe-dask-wrapper") as flow:
            run_pipeline_with_dask(pipeline)
        return flow

    @staticmethod
    def execute(flow: Flow):
        return flow.run()
