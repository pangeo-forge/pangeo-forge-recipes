from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from typing import Callable

from rechunker.executors import (
    DaskPipelineExecutor,
    PrefectPipelineExecutor,
    PythonPipelineExecutor,
)
from rechunker.types import ParallelPipelines

from ..patterns import FilePattern, prune_pattern


class BaseRecipe(ABC):
    """Base recipe class from which all other Recipes inherit.
    """

    @abstractmethod
    def _to_pipelines(self) -> ParallelPipelines:  # pragma: no cover
        raise NotImplementedError

    def to_function(self) -> Callable[[], None]:
        """
        Translate the recipe to a Python function for execution.
        """

        executor = PythonPipelineExecutor()
        return executor.pipelines_to_plan(self._to_pipelines())

    def to_dask(self):
        """
        Translate the recipe to a dask.Delayed object for parallel execution.
        """

        executor = DaskPipelineExecutor()
        return executor.pipelines_to_plan(self._to_pipelines())

    def to_prefect(self):
        """Compile the recipe to a Prefect.Flow object."""

        executor = PrefectPipelineExecutor()
        return executor.pipelines_to_plan(self._to_pipelines())

    # https://stackoverflow.com/questions/59986413/achieving-multiple-inheritance-using-python-dataclasses
    def __post_init__(self):
        # just intercept the __post_init__ calls so they
        # aren't relayed to `object`
        pass


@dataclass
class FilePatternRecipeMixin:
    file_pattern: FilePattern

    def copy_pruned(self, nkeep: int = 2):
        """Make a copy of this recipe with a pruned file pattern.

        :param nkeep: The number of items to keep from each ConcatDim sequence.
        """

        new_pattern = prune_pattern(self.file_pattern, nkeep=nkeep)
        return replace(self, file_pattern=new_pattern)
