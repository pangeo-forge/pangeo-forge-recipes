from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field, replace
from typing import Callable, ClassVar, Optional

import pkg_resources  # type: ignore

from ..executors.base import Pipeline
from ..patterns import FilePattern, prune_pattern
from ..serialization import dataclass_sha256
from ..storage import StorageConfig, temporary_storage_config


@dataclass
class BaseRecipe(ABC):
    _compiler: ClassVar[RecipeCompiler]
    _hash_exclude_ = ["storage_config"]
    sha256: Optional[bytes] = None

    def __post_init__(self):
        self.sha256 = dataclass_sha256(self, ignore_keys=self._hash_exclude_)

    def to_function(self):
        from ..executors import FunctionPipelineExecutor

        return FunctionPipelineExecutor.compile(self._compiler())

    def to_generator(self):
        from ..executors import GeneratorPipelineExecutor

        return GeneratorPipelineExecutor.compile(self._compiler())

    def to_dask(self):
        from pangeo_forge_recipes.executors import DaskPipelineExecutor

        return DaskPipelineExecutor.compile(self._compiler())

    def to_prefect(self, wrap_dask=False):
        from pangeo_forge_recipes.executors import (
            PrefectDaskWrapperExecutor,
            PrefectPipelineExecutor,
        )

        compiler = self._compiler()
        if wrap_dask:
            return PrefectDaskWrapperExecutor.compile(compiler)
        else:
            return PrefectPipelineExecutor.compile(compiler)

    def to_beam(self):
        from pangeo_forge_recipes.executors import BeamPipelineExecutor

        return BeamPipelineExecutor.compile(self._compiler())

    def get_execution_context(self):
        return dict(
            # See https://stackoverflow.com/a/2073599 re: version
            version=pkg_resources.require("pangeo-forge-recipes")[0].version,
            recipe_hash=self.sha256.hex(),
            inputs_hash=self.file_pattern.sha256.hex(),
        )


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
