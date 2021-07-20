import warnings
from abc import ABC, abstractmethod
from functools import partial
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

    @property
    @abstractmethod
    def prepare_target(self) -> Callable[[], None]:
        """Prepare the recipe for execution by initializing the target.
        Attribute that returns a callable function.
        """
        pass

    @abstractmethod
    def iter_inputs(self) -> Iterable[Hashable]:
        """Iterate over all inputs."""
        pass

    @property
    @abstractmethod
    def cache_input(self) -> Callable[[Hashable], None]:
        """Copy an input from its source location to the cache.
        Attribute that returns a callable function.
        """
        pass

    @abstractmethod
    def iter_chunks(self) -> Iterable[Hashable]:
        """Iterate over all target chunks."""
        pass

    @property
    @abstractmethod
    def store_chunk(self) -> Callable[[Hashable], None]:
        """Store a chunk of data in the target.
        Attribute that returns a callable function.
        """
        pass

    @property
    @abstractmethod
    def finalize_target(self) -> Callable[[], None]:
        """Final step to finish the recipe after data has been written.
        Attribute that returns a callable function.
        """
        pass

    def to_pipelines(self) -> ParallelPipelines:
        """Translate recipe to pipeline for execution.
        """
        warnings.warn(
            "'to_pipelines' is deprecated. Use one of 'to_function', 'to_dask', or "
            "'to_prefect' directly instead.",
            FutureWarning,
        )
        pipeline = []  # type: MultiStagePipeline
        # TODO: allow recipes to customize which stages to run
        pipeline.append(Stage(self.cache_input, list(self.iter_inputs())))
        pipeline.append(Stage(self.prepare_target))
        pipeline.append(Stage(self.store_chunk, list(self.iter_chunks())))
        pipeline.append(Stage(self.finalize_target))
        pipelines = []  # type: ParallelPipelines
        pipelines.append(pipeline)
        return pipelines

    def to_function(self) -> Callable[[], None]:
        """
        Translate the recipe to a Python function for execution.
        """

        def pipeline():
            # TODO: allow recipes to customize which stages to run
            for input_key in self.iter_inputs():
                self.cache_input(input_key)
            self.prepare_target()
            for chunk_key in self.iter_chunks():
                self.store_chunk(chunk_key)
            self.finalize_target()

        return pipeline

    def to_dask(self):
        """
        Translate the recipe to a dask.Delayed object for parallel execution.
        """
        # This manually builds a Dask task graph with each stage of the recipe.
        # We use a few "checkpoints" to ensure that downstream tasks depend
        # on upstream tasks being done before starting. We use a manual task
        # graph rather than dask.delayed to avoid some expensive tokenization
        # in dask.delayed
        import dask
        from dask.delayed import Delayed

        # TODO: HighlevelGraph layers for each of these mapped inputs.
        # Cache Input --------------------------------------------------------
        dsk = {}
        token = dask.base.tokenize(self)

        # TODO: allow recipes to customize which stages to run
        for i, input_key in enumerate(self.iter_inputs()):
            dsk[(f"cache_input-{token}", i)] = (self.cache_input, input_key)

        # Prepare Target ------------------------------------------------------
        dsk[f"checkpoint_0-{token}"] = (lambda *args: None, list(dsk))
        dsk[f"prepare_target-{token}"] = (
            _prepare_target,
            f"checkpoint_0-{token}",
            self.prepare_target,
        )

        # Store Chunk --------------------------------------------------------
        keys = []
        for i, chunk_key in enumerate(self.iter_chunks()):
            k = (f"store_chunk-{token}", i)
            dsk[k] = (_store_chunk, f"prepare_target-{token}", self.store_chunk, chunk_key)
            keys.append(k)

        # Finalize Target -----------------------------------------------------
        dsk[f"checkpoint_1-{token}"] = (lambda *args: None, keys)
        key = f"finalize_target-{token}"
        dsk[key] = (_finalize_target, f"checkpoint_1-{token}", self.finalize_target)

        return Delayed(key, dsk)

    def to_prefect(self):
        """Compile the recipe to a Prefect.Flow object."""
        from prefect import Flow, task, unmapped

        # TODO: allow recipes to customize which stages to run
        cache_input_task = task(self.cache_input, name="cache_input")
        prepare_target_task = task(self.prepare_target, name="prepare_target")
        store_chunk_task = task(self.store_chunk, name="store_chunk")
        finalize_target_task = task(self.finalize_target, name="finalize_target")

        with Flow("pangeo-forge-recipe") as flow:
            cache_task = cache_input_task.map(input_key=list(self.iter_inputs()))
            upstream_tasks = [cache_task]
            prepare_task = prepare_target_task(upstream_tasks=upstream_tasks)
            store_task = store_chunk_task.map(
                chunk_key=list(self.iter_chunks()), upstream_tasks=[unmapped(prepare_task)],
            )
            _ = finalize_target_task(upstream_tasks=[store_task])

        return flow

    def __iter__(self):
        # TODO: allow recipes to customize which stages to run
        yield self.cache_input, self.iter_inputs()
        yield self.prepare_target, []
        yield self.store_chunk, self.iter_chunks()
        yield self.finalize_target, []

    # https://stackoverflow.com/questions/59986413/achieving-multiple-inheritance-using-python-dataclasses
    def __post_init__(self):
        # just intercept the __post_init__ calls so they
        # aren't relayed to `object`
        pass


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


def _prepare_target(checkpoint, func):
    return func()


def _store_chunk(checkpoint, func, input_key):
    return func(input_key)


def _finalize_target(checkpoint, func):
    return func()
