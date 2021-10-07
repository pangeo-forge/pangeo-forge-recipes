from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Callable, Any, Iterable, List, Optional
from enum import Enum, auto


class StageAnnotation(Enum):
    CONCURRENCY = auto()
    RETRIES = auto()


Config = Any


@dataclass(frozen=True)
class Stage:
    function: Callable
    name: str
    mappable: Optional[Iterable] = None
    config: Optional[Config] = None
    annotations: Optional[Dict[StageAnnotation, Any]] = None


Pipeline = List[Stage]


def pipeline_to_function():
    pass
