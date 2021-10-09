from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from typing import Callable, ClassVar, Hashable, Iterable

from ..executors.base import Pipeline
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


@dataclass
class BaseRecipe(ABC):
    _compiler: ClassVar[RecipeCompiler]

    def to_function(self):
        from ..executors.function import FunctionPipelineExecutor

        return FunctionPipelineExecutor.compile(self._compiler())

    def to_dask(self):
        from pangeo_forge_recipes.executors.dask import DaskPipelineExecutor

        return DaskPipelineExecutor.compile(self._compiler())

    def to_prefect(self):
        from pangeo_forge_recipes.executors.prefect import PrefectPipelineExecutor

        return PrefectPipelineExecutor.compile(self._compiler())


RecipeCompiler = Callable[[BaseRecipe], Pipeline]


@dataclass
class FilePatternMixin:
    file_pattern: FilePattern

    def copy_pruned(self, nkeep: int = 2):
        """Make a copy of this recipe with a pruned file pattern.

        :param nkeep: The number of items to keep from each ConcatDim sequence.
        """

        new_pattern = prune_pattern(self.file_pattern, nkeep=nkeep)
        return replace(self, file_pattern=new_pattern)
