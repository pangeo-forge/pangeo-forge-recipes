from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, replace
from typing import Callable, ClassVar

from ..executors.base import Pipeline
from ..patterns import FilePattern, prune_pattern


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
