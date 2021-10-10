import enum
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, Hashable, Iterable, Optional, Protocol, TypeVar

Config = Any


# https://stackoverflow.com/questions/57837609/python-typing-signature-typing-callable-for-function-with-kwargs
class NoArgumentStageFunction(Protocol):
    def __call__(*, config: Optional[Config] = None) -> None:
        ...


class SingleArgumentStageFunction(Protocol):
    def __call__(__a: Hashable, *, config: Optional[Config] = None) -> None:
        ...


# For some reason, mypy does not like this
# StageFunction = Union[NoArgumentStageFunction, SingleArgumentStageFunction]
# pangeo_forge_recipes/recipes/xarray_zarr.py:525: error:
#  Argument "function" to "Stage" has incompatible type
#    "Callable[[Index, NamedArg(XarrayZarrRecipe, 'config')], None]";
#    expected "NoArgumentStageFunction"  [arg-type]

# TODO: fix this to be a stricter type as above
StageFunction = Callable


class StageAnnotationType(enum.Enum):
    CONCURRENCY = enum.auto()
    RETRIES = enum.auto()


StageAnnotations = Dict[StageAnnotationType, Any]


@dataclass(frozen=True)
class Stage:
    function: StageFunction
    name: str
    mappable: Optional[Iterable] = None
    annotations: Optional[StageAnnotations] = None


@dataclass(frozen=True)
class Pipeline:
    stages: Iterable[Stage]
    config: Optional[Config] = None


T = TypeVar("T")


class PipelineExecutor(Generic[T]):
    @staticmethod
    def compile(pipeline: Pipeline) -> T:
        raise NotImplementedError

    @staticmethod
    def execute(plan: T):
        raise NotImplementedError
