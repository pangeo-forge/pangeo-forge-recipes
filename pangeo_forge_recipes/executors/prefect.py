from typing import Any, Callable, Dict, Optional

from prefect import Flow, task, unmapped

from .base import Pipeline, PipelineExecutor, StageAnnotations, StageAnnotationType


def annotations_to_task_kwargs(annotations: Optional[StageAnnotations]) -> Dict[str, Any]:
    task_kwargs = {}
    for ann_type in annotations or []:
        if ann_type == StageAnnotationType.retries:
            task_kwargs["max_retries"] = annotations[ann_type]
        if ann_type == StageAnnotationType.concurrency:
            raise ValueError("Haven't figured out concurrency yet.")
    return task_kwargs


class PrefectPipelineExecutor(PipelineExecutor[Flow]):
    @staticmethod
    def compile(pipeline: Pipeline):

        with Flow("pangeo-forge-recipe") as flow:
            upstream_tasks = None
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
