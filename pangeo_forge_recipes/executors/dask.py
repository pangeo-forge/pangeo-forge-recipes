from typing import Any, Dict, Set, Tuple, Union

import dask
from dask.delayed import Delayed
from dask.highlevelgraph import HighLevelGraph

from .base import Pipeline, PipelineExecutor


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


def append_token(task_name: str, token: str) -> str:
    return f"{task_name}-{token}"


class DaskPipelineExecutor(PipelineExecutor[Delayed]):
    @staticmethod
    def compile(pipeline: Pipeline):

        token = dask.base.tokenize(pipeline)

        # we are constructing a HighLevelGraph from scratch
        # https://docs.dask.org/en/latest/high-level-graphs.html
        layers = dict()  # type: Dict[str, Dict[Union[str, Tuple[str, int]], Any]]
        dependencies = dict()  # type: Dict[str, Set[str]]

        # start with just the config as a standalone layer
        # create a custom delayed object for the config
        config_task = "config"
        config_token = append_token(config_task, token)
        layers[config_task] = {config_token: pipeline.config}
        dependencies[config_task] = set()

        prev_token = ()  # type: Tuple[str, ...]
        prev_task = config_task
        for stage in pipeline.stages:
            stage_token = append_token(stage.name, token)
            stage_graph = {}  # type: Dict[Union[str, Tuple[str, int]], Any]
            if stage.mappable is None:
                func = wrap_standalone_task(stage.function)
                stage_graph[stage_token] = (func, config_token) + prev_token
                prev_token = (stage_token,)
            else:
                func = wrap_map_task(stage.function)
                checkpoint_args = []
                for i, m in enumerate(stage.mappable):
                    key = (stage.name, i)
                    stage_graph[key] = (func, m, config_token) + prev_token
                    checkpoint_args.append(key)
                checkpoint_key = f"{stage.name}-checkpoint-{token}"
                stage_graph[checkpoint_key] = (checkpoint, *checkpoint_args)
                prev_token = (checkpoint_key,)
            layer_name = prev_token[0]
            layers[layer_name] = stage_graph
            dependencies[layer_name] = {prev_task}
            prev_task = stage.name

        hlg = HighLevelGraph(layers, dependencies)
        delayed = Delayed(layer_name, hlg)
        return delayed

    @staticmethod
    def execute(delayed: Delayed):
        delayed.compute()
