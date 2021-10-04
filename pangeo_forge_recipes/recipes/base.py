from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from typing import Callable, Hashable, Iterable

from ..patterns import FilePattern, prune_pattern

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


class RecipeConfig(ABC):
    pass


class BaseRecipe(ABC):
    """Base recipe class from which all other Recipes inherit.
    """

    @property
    @abstractmethod
    def prepare_target(self) -> Callable[[RecipeConfig], None]:
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
    def cache_input(self) -> Callable[[RecipeConfig, Hashable], None]:
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
    def store_chunk(self) -> Callable[[RecipeConfig, Hashable], None]:
        """Store a chunk of data in the target.
        Attribute that returns a callable function.
        """
        pass

    @property
    @abstractmethod
    def finalize_target(self) -> Callable[[RecipeConfig], None]:
        """Final step to finish the recipe after data has been written.
        Attribute that returns a callable function.
        """
        pass

    def to_function(self) -> Callable[[], None]:
        """
        Translate the recipe to a Python function for execution.
        """

        def pipeline():
            # TODO: allow recipes to customize which stages to run
            for input_key in self.iter_inputs():
                self.cache_input(self.config, input_key)
            self.prepare_target(self.config)
            for chunk_key in self.iter_chunks():
                self.store_chunk(self.config, chunk_key)
            self.finalize_target(self.config)

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

        config_ = f"config-{token}"
        dsk[config_] = self.config

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

    def to_prefect(self):
        """Compile the recipe to a Prefect.Flow object."""
        from prefect import Flow, task, unmapped

        # TODO: allow recipes to customize which stages to run
        cache_input_task = task(self.cache_input, name="cache_input")
        prepare_target_task = task(self.prepare_target, name="prepare_target")
        store_chunk_task = task(self.store_chunk, name="store_chunk")
        finalize_target_task = task(self.finalize_target, name="finalize_target")

        with Flow("pangeo-forge-recipe") as flow:
            # we hope this will be interpreted as a Prefect Constant
            # https://docs.prefect.io/core/concepts/tasks.html#constants
            config = self.config
            cache_task = cache_input_task.map(
                config=unmapped(config), input_key=list(self.iter_inputs())
            )
            upstream_tasks = [cache_task]
            prepare_task = prepare_target_task(config=config, upstream_tasks=upstream_tasks)
            store_task = store_chunk_task.map(
                config=unmapped(config),
                chunk_key=list(self.iter_chunks()),
                upstream_tasks=[unmapped(prepare_task)],
            )
            _ = finalize_target_task(config=config, upstream_tasks=[store_task])

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


def _checkpoint(checkpoint, func, *args, **kwargs):
    return func(*args, **kwargs)


@dataclass
class FilePatternRecipeMixin:
    file_pattern: FilePattern

    def copy_pruned(self, nkeep: int = 2):
        """Make a copy of this recipe with a pruned file pattern.

        :param nkeep: The number of items to keep from each ConcatDim sequence.
        """

        new_pattern = prune_pattern(self.file_pattern, nkeep=nkeep)
        return replace(self, file_pattern=new_pattern)
