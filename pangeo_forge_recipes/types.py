from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Tuple, TypeVar


class CombineOp(Enum):
    """Used to uniquely identify different combine operations across Pangeo Forge Recipes."""

    MERGE = 1
    CONCAT = 2
    SUBSET = 3


@dataclass(frozen=True, order=True)
class Dimension:
    """
    :param name: The name of the dimension we are combining over.
    :param operation: What type of combination this is (merge or concat)
    """

    name: str
    operation: CombineOp


@dataclass(order=True)
class Position:
    """
    :param indexed: If True, this position represents an offset within a dataset
       If False, it is a position within a sequence.
    """

    value: int
    # TODO: consider using a ClassVar here
    indexed: bool = False
    filename: str = ""


@dataclass(order=True)
class IndexedPosition(Position):
    indexed: bool = True
    dimsize: int = 0


class Index(Dict[Dimension, Position]):
    """An Index is a special sort of dictionary which describes a position within
    a multidimensional set.

    - The key is a :class:`Dimension` which tells us which dimension we are addressing.
    - The value is a :class:`Position` which tells us where we are within that dimension.

    This object is hashable and deterministically serializable.
    """

    def __hash__(self):
        return hash(tuple(self.__getstate__()))

    def __getstate__(self):
        return sorted(self.items())

    def __setstate__(self, state):
        self.__init__({k: v for k, v in state})

    def find_concat_dim(self, dim_name: str) -> Optional[Dimension]:
        possible_concat_dims = [
            d for d in self if (d.name == dim_name and d.operation == CombineOp.CONCAT)
        ]
        if len(possible_concat_dims) > 1:
            raise ValueError(
                f"Found {len(possible_concat_dims)} concat dims named {dim_name} "
                f"in the index {self}."
            )
        elif len(possible_concat_dims) == 0:
            return None
        else:
            return possible_concat_dims[0]

    def find_position(self, dim_name: str) -> int:
        dimension = self.find_concat_dim(dim_name)
        if dimension:
            return self[dimension].value
        else:
            raise ValueError(f"No dimension found with name {dim_name}")


# A convenience type to represent an indexed value
T = TypeVar("T")
Indexed = Tuple[Index, T]
