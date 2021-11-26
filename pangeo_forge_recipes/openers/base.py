from abc import ABC, abstractmethod
from typing import ContextManager, Generic, TypeVar

T_in = TypeVar("T_in")
T_out = TypeVar("T_out")


# How to make an ABC with a contextmanager method
# https://stackoverflow.com/a/69674704/3266235
class BaseOpener(ABC, Generic[T_in, T_out]):
    @abstractmethod
    def open(self, input: T_in) -> ContextManager[T_out]:
        """Should be a Contextmanager which yields the output type
        """
        pass
