import warnings
from abc import ABC, abstractmethod
from functools import partial, reduce
from typing import Callable, Hashable, Iterable

from rechunker.types import MultiStagePipeline, ParallelPipelines, Stage

# How to manually execute a recipe: ###
#
#   t = PangeoForgeTarget()
#   r = MyRecipe(target=t, **opts) # 1
#   # manual execution of recipe
#   for input_key in r.iter_inputs():
#       r.cache_input(input_key) # 4
#   r.prepare_target() # 3
#   for chunk_key in r.iter_chunks():
#       r.store_chunk(chunk_key) # 5
#   r.finalize_target() # 6


# 1) Initialize the Recipe object
# 2) Point the Recipe at its Target
# 3) Initialize the recipe.
#    Check if the target exists; if not, create it.
# 4) cache the inputs to proximate storage (OPTIONAL)
#    Some recipes won't need this (e.g. cloud to cloud)
#    If so, iter_inputs is just an empty iterator
# 5) Load each chunk from the inputs and store it in the target
#    Might be coming from the cache or might be read directly.
# 6)


class BaseRecipe(ABC):
    """Base recipe class from which all other Recipes inherit.
    """

    @abstractmethod
    def prepare_target(self) -> None:
        """Prepare the recipe for execution by initializing the target.
        Attribute that returns a callable function.
        """
        pass

    @abstractmethod
    def iter_inputs(self) -> Iterable[Hashable]:
        """Iterate over all inputs."""
        pass

    @abstractmethod
    def cache_input(self) -> None:
        """Copy an input from its source location to the cache.
        Attribute that returns a callable function.
        """
        pass

    @abstractmethod
    def iter_chunks(self) -> Iterable[Hashable]:
        """Iterate over all target chunks."""
        pass

    @abstractmethod
    def store_chunk(self) -> None:
        """Store a chunk of data in the target.
        Attribute that returns a callable function.
        """
        pass

    @abstractmethod
    def finalize_target(self) -> None:
        """Final step to finish the recipe after data has been written.
        Attribute that returns a callable function.
        """
        pass

    def to_pipelines(self) -> ParallelPipelines:
        """Translate recipe to pipeline for execution.
        """

        warnings.warn(
            "This function will be removed from future versions. "
            "Use `to_dask` or `to_prefect` instead.",
            DeprecationWarning,
        )

        pipeline = []  # type: MultiStagePipeline
        if getattr(self, "cache_inputs", False):  # TODO: formalize this contract
            pipeline.append(Stage(self.cache_input, list(self.iter_inputs())))
        pipeline.append(Stage(self.prepare_target))
        pipeline.append(Stage(self.store_chunk, list(self.iter_chunks())))
        pipeline.append(Stage(self.finalize_target))
        pipelines = []  # type: ParallelPipelines
        pipelines.append(pipeline)
        return pipelines

    # https://stackoverflow.com/questions/59986413/achieving-multiple-inheritance-using-python-dataclasses
    def __post_init__(self):
        # just intercept the __post_init__ calls so they
        # aren't relayed to `object`
        pass

    def to_dask(self):
        """Compile the recipe to a Dask.Delayed object."""

        from dask import delayed
        from dask.graph_manipulation import bind, checkpoint

        cls_ = self.__class__

        def self_task():
            return self

        self_ = delayed(self_task)()

        layers = [
            [delayed(cls_.cache_input)(self_, input_key) for input_key in self.iter_inputs()],
            [delayed(cls_.prepare_target)(self_)],
            [delayed(cls_.store_chunk)(self_, chunk_key) for chunk_key in self.iter_chunks()],
            [delayed(cls_.finalize_target)(self_)],
        ]

        def combine(children, parents):
            return checkpoint(bind(children, parents, assume_layers=False))

        return reduce(combine, layers[::-1])

    def to_prefect(self):
        """Compile the recipe to a Prefect.Flow object."""

        from prefect import Flow, task, unmapped

        cls_ = self.__class__

        @task
        def recipe():
            return self

        @task
        def cache_input(recipe_obj, input_key):
            cls_.cache_input(recipe_obj, input_key)

        @task
        def prepare_target(recipe_obj):
            cls_.prepare_target(recipe_obj)

        @task
        def store_chunk(recipe_obj, chunk_key):
            cls_.store_chunk(recipe_obj, chunk_key)

        @task
        def finalize_target(recipe_obj):
            cls_.finalize_target(recipe_obj)

        with Flow("pangeo-forge-recipe") as flow:
            recipe_ = recipe()
            cache_task = cache_input.map(
                recipe_obj=unmapped(recipe_), input_key=list(self.iter_inputs())
            )
            prepare_task = prepare_target(recipe_obj=recipe_, upstream_tasks=[cache_task])
            store_task = store_chunk.map(
                recipe_obj=unmapped(recipe_),
                chunk_key=list(self.iter_chunks()),
                upstream_tasks=[unmapped(prepare_task)],
            )
            _ = finalize_target(recipe_obj=recipe_, upstream_tasks=[store_task])

        return flow

    def to_function(self):
        """Compile the recipe to a single python function."""

        cls_ = self.__class__

        tasks = [partial(cls_.cache_input, self, input_key) for input_key in self.iter_inputs()]
        tasks += [partial(cls_.prepare_target, self)]
        tasks += [partial(cls_.store_chunk, self, chunk_key) for chunk_key in self.iter_chunks()]
        tasks += [partial(cls_.finalize_target, self)]

        def _execute_all(tasks):
            for task in tasks:
                task()

        return partial(_execute_all, tasks)


def closure(func: Callable) -> Callable:
    """Wrap a method to eliminate the self keyword from its signature."""

    # tried using @functools.wraps, but could not get it to work right
    def wrapped(*args, **kwargs):
        self = args[0]
        if len(args) > 1:
            args = args[1:]
        else:
            args = ()
        new_func = partial(func, self, *args, **kwargs)
        new_func.__name__ = getattr(func, "__name__", None)
        return new_func

    return wrapped
