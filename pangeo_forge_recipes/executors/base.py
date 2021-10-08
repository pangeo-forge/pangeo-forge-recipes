from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Generic, Any, Iterable, Optional, Protocol, Hashable, Union, TypeVar
import enum
from typing import Protocol

Config = Any

# https://stackoverflow.com/questions/57837609/python-typing-signature-typing-callable-for-function-with-kwargs
class NoArgumentStageFunction(Protocol):
    def __call__(*, config: Optional[Config]=None) -> None: ...


class SingleArgumentStageFunction(Protocol):
    def __call__(__a: Hashable, *, config: Optional[Config]=None) -> None: ...


StageFunction = Union[NoArgumentStageFunction, SingleArgumentStageFunction]


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
