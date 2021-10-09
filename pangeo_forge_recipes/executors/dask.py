from pprint import pprint
from typing import Any, Callable, Dict, Optional

import dask
import dask.bag as db
from dask.delayed import Delayed
from dask.graph_manipulation import bind

from .base import Pipeline, PipelineExecutor

# task = (apply, func, args, kwargs)  # func(*args, **kwargs)



def wrap_map_task(function):
    # dependencies are dummy args used to create dependence between stages
    def wrapped(map_arg, config, *dependencies):
        return function(map_arg, config=config)

    return wrapped


def wrap_standalone_task(function):
    def wrapped(config, *dependencies):
        return function(config=config)

    return wrapped


def checkpoint(*args):
    return


class DaskPipelineExecutor(PipelineExecutor[Delayed]):
    @staticmethod
    def compile(pipeline: Pipeline):

        token = dask.base.tokenize(pipeline)
        # create a custom delayed object for the config
        config_task = f"config-{token}"
        dsk = {config_task: pipeline.config}

        prev_layer = tuple()
        for stage in pipeline.stages:
            stage_name = f"{stage.name}-{token}"
            if stage.mappable is None:
                func = wrap_standalone_task(stage.function)
                dsk[stage_name] = (func, config_task) + prev_layer
                prev_layer = (stage_name,)
            else:
                func = wrap_map_task(stage.function)
                checkpoint_args = []
                for i, m in enumerate(stage.mappable):
                    key = (stage_name, i)
                    dsk[key] = (func, m, config_task) + prev_layer
                    checkpoint_args.append(key)
                checkpoint_key = f"{stage.name}-checkpoint-{token}"
                dsk[checkpoint_key] = (checkpoint, *checkpoint_args)
                prev_layer = (checkpoint_key,)

        delayed = Delayed(checkpoint_key, dsk)
        return delayed

    @staticmethod
    def execute(delayed: Delayed):
        delayed.compute()
