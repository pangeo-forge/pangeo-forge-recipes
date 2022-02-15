from __future__ import annotations

import tempfile
from abc import ABC
from dataclasses import dataclass, replace
from typing import Callable, ClassVar

from fsspec.implementations.local import LocalFileSystem

from ..executors.base import Pipeline
from ..patterns import FilePattern, prune_pattern
from ..storage import CacheFSSpecTarget, FSSpecTarget, MetadataTarget


@dataclass
class BaseRecipe(ABC):
    _compiler: ClassVar[RecipeCompiler]

    def to_function(self):
        from ..executors import FunctionPipelineExecutor

        return FunctionPipelineExecutor.compile(self._compiler())

    def to_generator(self):
        from ..executors import GeneratorPipelineExecutor

        return GeneratorPipelineExecutor.compile(self._compiler())

    def to_dask(self):
        from pangeo_forge_recipes.executors import DaskPipelineExecutor

        return DaskPipelineExecutor.compile(self._compiler())

    def to_prefect(self):
        from pangeo_forge_recipes.executors import PrefectPipelineExecutor

        return PrefectPipelineExecutor.compile(self._compiler())

    def to_beam(self):
        from pangeo_forge_recipes.executors import BeamPipelineExecutor

        return BeamPipelineExecutor.compile(self._compiler())


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


@dataclass
class TemporaryStorageMixin:
    def assign_temporary_storage(self):
        """Assign temporary storage targets to a recipe instance for local testing and debugging.
        """

        fs_local = LocalFileSystem()

        for target, storage_cls in {
            "target": FSSpecTarget,
            "input_cache": CacheFSSpecTarget,
            "metadata_cache": MetadataTarget,
        }.items():
            if hasattr(self, target):
                temp_dir = tempfile.TemporaryDirectory()
                setattr(self, target, storage_cls(fs_local, temp_dir.name))
