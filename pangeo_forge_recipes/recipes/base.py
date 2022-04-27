from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field, replace
from typing import Callable, ClassVar

from ..executors.base import Pipeline
from ..patterns import FilePattern, prune_pattern
from ..storage import StorageConfig, temporary_storage_config


@dataclass
class BaseRecipe(ABC):
    _compiler: ClassVar[RecipeCompiler]
    _hash_exclude_ = ["file_pattern", "storage_config"]

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

    def sha256(self):
        from ..serialization import dataclass_sha256

        return dataclass_sha256(self, ignore_keys=self._hash_exclude_)


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
class StorageMixin:
    """Provides the storage configuration for Pangeo Forge recipe classes.

    :param storage_config: The storage configuration.
    """

    storage_config: StorageConfig = field(default_factory=temporary_storage_config)

    @property
    def target(self):
        return f"{self.storage_config.target.fs.protocol}://{self.storage_config.target.root_path}"

    @property
    def target_mapper(self):
        return self.storage_config.target.get_mapper()
