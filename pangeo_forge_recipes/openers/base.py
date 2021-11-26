from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Generator

T_in = TypeVar('T_in')
T_out = TypeVar('T_out')


class BaseOpener(ABC, Generic[T_in, T_out]):

    @abstractmethod
    def open(input: T_in) -> Generator[T_out, None, None]:
        """Should be a Contextmanager which yields the output type
        """
        pass
