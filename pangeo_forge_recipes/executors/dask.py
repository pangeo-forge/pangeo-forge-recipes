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
        config_key = append_token("config", token)
        layers[config_key] = {config_key: pipeline.config}
        dependencies[config_key] = set()

        prev_key = config_key
        prev_dependency = ()  # type: Union[Tuple[()], Tuple[str]]
        for stage in pipeline.stages:
            stage_graph = {}  # type: Dict[Union[str, Tuple[str, int]], Any]
            # each block should
            if stage.mappable is None:
                stage_key = append_token(stage.name, token)
                func = wrap_standalone_task(stage.function)
                stage_graph[stage_key] = (func, config_key) + prev_dependency
            else:
                func = wrap_map_task(stage.function)
                checkpoint_args = []
                for i, m in enumerate(stage.mappable):
                    key = (append_token(stage.name, token), i)
                    stage_graph[key] = (func, m, config_key) + prev_dependency
                    checkpoint_args.append(key)
                stage_key = f"{stage.name}-checkpoint-{token}"
                stage_graph[stage_key] = (checkpoint, *checkpoint_args)
            layers[stage_key] = stage_graph
            dependencies[stage_key] = {config_key} | {prev_key}
            prev_dependency = (stage_key,)
            prev_key = stage_key

        hlg = HighLevelGraph(layers, dependencies)
        delayed = Delayed(prev_key, hlg)
        return delayed

    @staticmethod
    def execute(delayed: Delayed):
        delayed.compute()
