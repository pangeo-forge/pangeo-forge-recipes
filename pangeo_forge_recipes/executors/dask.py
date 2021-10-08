from typing import Callable, Dict, Any, Optional

from dask.delayed import Delayed
import dask.bag as db
import dask.delayed
from dask.graph_manipulation import bind
from dask.compatibility import apply
# task = (apply, func, args, kwargs)  # func(*args, **kwargs)

from .base import Pipeline, PipelineExecutor, StageAnnotations, StageAnnotationType


class DaskPipelineExecutor(PipelineExecutor[Delayed]):

    @staticmethod
    def compile(pipeline: Pipeline):

        token = dask.base.tokenize(pipeline)
        # create a custom delayed object for the config
        config_task = f'config-{token}'
        config_delayed = Delayed(config_task, {config_task:  pipeline.config})
        config_bag = db.from_delayed(config_delayed)

        prev_layer = None
        for stage in pipeline.stages:
            kwargs = {'config': stage.config} if stage.config else {}
            if stage.mappable is None:
                layer = db.from_delayed(dask.delayed(stage.function)(**kwargs))
            else:
                layer = db.map(stage.function, db.from_sequence(layer.mappable), **kwargs)

                for i, m in enumerate(stage.mappable):
                    dsk[(stage_name, i)] = (apply, stage.function, (m,), kwargs)


        # TODO: allow recipes to customize which stages to run
        for i, input_key in enumerate(self.iter_inputs()):
            dsk[(f"cache_input-{token}", i)] = (self.cache_input, config_, input_key)

        # Prepare Target ------------------------------------------------------
        dsk[f"checkpoint_0-{token}"] = (lambda *args: None, list(dsk))
        dsk[f"prepare_target-{token}"] = (
            _checkpoint,
            f"checkpoint_0-{token}",
            self.prepare_target,
            config_,
        )

        # Store Chunk --------------------------------------------------------
        keys = []
        for i, chunk_key in enumerate(self.iter_chunks()):
            k = (f"store_chunk-{token}", i)
            dsk[k] = (
                _checkpoint,
                f"prepare_target-{token}",
                self.store_chunk,
                config_,
                chunk_key,
            )
            keys.append(k)

        # Finalize Target -----------------------------------------------------
        dsk[f"checkpoint_1-{token}"] = (lambda *args: None, keys)
        key = f"finalize_target-{token}"
        dsk[key] = (_checkpoint, f"checkpoint_1-{token}", self.finalize_target, config_)

        return Delayed(key, dsk)


        with Flow("pangeo-forge-recipe") as flow:
            upstream_tasks = None
            for stage in pipeline.stages:
                task_kwargs = annotations_to_task_kwargs(stage.annotations)
                stage_task = task(stage.function, name=stage.name, **task_kwargs)
                if stage.mappable is not None:
                    stage_task_called = stage_task.map(
                        stage.mappable,
                        config=unmapped(pipeline.config),
                        upstream_tasks=[unmapped(t) for t in upstream_tasks]
                    )
                else:
                    stage_task_called = stage_task(
                        config=pipeline.config,
                        upstream_tasks=upstream_tasks
                    )
                upstream_tasks = [stage_task_called]

        return flow

    @staticmethod
    def execute(flow: Flow):
        return flow.run()



def checkpoint(checkpoint, func, *args, **kwargs):
    return func(*args, **kwargs)
