"""
Filename / URL patterns.
"""

from dataclasses import dataclass, field, replace
from enum import Enum
from itertools import product
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)


class CombineOp(Enum):
    """Used to uniquely identify different combine operations across Pangeo Forge Recipes.
    """

    MERGE = 1
    CONCAT = 2
    SUBSET = 3


@dataclass(frozen=True)
class ConcatDim:
    """Represents a concatenation operation across a dimension of a FilePattern.

    :param name: The name of the dimension we are concatenating over. For
      files with labeled dimensions, this should match the dimension name
      within the file. The most common value is ``"time"``.
    :param keys: The keys used to represent each individual item along this
      dimension. This will be used by a ``FilePattern`` object to evaluate
      the file name.
    :param nitems_per_file: If each file contains the exact same known number of
      items in each file along the concat dimension, this can be set to
      provide a fast path for recipes.
    """

    name: str  # should match the actual dimension name
    keys: Sequence[Any] = field(repr=False)
    nitems_per_file: Optional[int] = None
    operation: ClassVar[CombineOp] = CombineOp.CONCAT


@dataclass(frozen=True)
class MergeDim:
    """Represents a merge operation across a dimension of a FilePattern.

    :param name: The name of the dimension we are are merging over. The actual
       value is not used by most recipes. The most common value is
       ``"variable"``.
    :param keys: The keys used to represent each individual item along this
      dimension. This will be used by a ``FilePattern`` object to evaluate
      the file name.
    """

    name: str
    keys: Sequence[Any] = field(repr=False)
    operation: ClassVar[CombineOp] = CombineOp.MERGE


@dataclass(frozen=True)
class DimIndex:
    """Object used to index a single dimension of a FilePattern or Recipe Chunks.

    :param name: The name of the dimension.
    :param index: The position of the item within the sequence.
    :param sequence_len: The total length of the sequence.
    :param operation: What type of Combine Operation does this dimension represent.
    """

    name: str
    index: int
    sequence_len: int
    operation: CombineOp

    def __str__(self):
        return f"{self.name}-{self.index}"

    def __post_init__(self):
        assert self.sequence_len > 0
        assert self.index >= 0
        assert self.index < self.sequence_len


class Index(tuple):
    """A tuple of ``DimIndex`` objects.
    The order of the indexes doesn't matter for comparision."""

    def __new__(self, args: Iterable[DimIndex]):
        # This validation really slows things down because we call Index a lot!
        # if not all((isinstance(a, DimIndex) for a in args)):
        #     raise ValueError("All arguments must be DimIndex.")
        # args_set = set(args)
        # if len(set(args_set)) < len(tuple(args)):
        #     raise ValueError("Duplicate argument detected.")
        return tuple.__new__(Index, args)

    def __str__(self):
        return ",".join(str(dim) for dim in self)

    def __eq__(self, other):
        return (set(self) == set(other)) and (len(self) == len(other))

    def __hash__(self):
        return hash(frozenset(self))


CombineDim = Union[MergeDim, ConcatDim]
FilePatternIndex = Index


class FilePattern:
    """Represents an n-dimensional matrix of individual files to be combined
    through a combination of merge and concat operations. Each operation generates
    a new dimension to the matrix.

    :param format_function: A function that takes one argument for each
      combine_op and returns a string representing the filename / url paths.
      Each argument name should correspond to a ``name`` in the ``combine_dims``
      list.
    :param combine_dims: A sequence of either concat or merge dimensions. The outer
      product of the keys is used to generate the full list of file paths.
    :param fsspec_open_kwargs: Extra options for opening the inputs with fsspec.
      May include ``block_size``, ``username``, ``password``, etc.
    :param query_string_secrets: If provided, these key/value pairs are appended to
      the query string of each ``file_pattern`` url at runtime.
    :param is_opendap: If True, assume all input fnames represent opendap endpoints.
      Cannot be used with caching.
    """

    def __init__(
        self,
        format_function: Callable,
        *combine_dims: CombineDim,
        fsspec_open_kwargs: Optional[Dict[str, Any]] = None,
        query_string_secrets: Optional[Dict[str, str]] = None,
        is_opendap: bool = False,
    ):
        self.format_function = format_function
        self.combine_dims = combine_dims
        self.fsspec_open_kwargs = fsspec_open_kwargs if fsspec_open_kwargs else {}
        self.query_string_secrets = query_string_secrets if query_string_secrets else {}
        self.is_opendap = is_opendap
        if self.fsspec_open_kwargs and self.is_opendap:
            raise ValueError(
                "OPeNDAP inputs are not opened with `fsspec`. "
                "`is_opendap` must be `False` when passing `fsspec_open_kwargs`."
            )

    def __repr__(self):
        return f"<FilePattern {self.dims}>"

    @property
    def dims(self) -> Dict[str, int]:
        """Dictionary representing the dimensions of the FilePattern. Keys are
        dimension names, values are the number of items along each dimension."""
        return {op.name: len(op.keys) for op in self.combine_dims}

    @property
    def shape(self) -> Tuple[int, ...]:
        """Shape of the filename matrix."""
        return tuple([len(op.keys) for op in self.combine_dims])

    @property
    def merge_dims(self) -> List[str]:
        """List of dims that are merge operations"""
        return [op.name for op in self.combine_dims if op.operation == CombineOp.MERGE]

    @property
    def concat_dims(self) -> List[str]:
        """List of dims that are concat operations"""
        return [op.name for op in self.combine_dims if op.operation == CombineOp.CONCAT]

    @property
    def nitems_per_input(self) -> Dict[str, Union[int, None]]:
        """Dictionary mapping concat dims to number of items per file."""
        nitems = {}  # type: Dict[str, Union[int, None]]
        for op in self.combine_dims:
            if isinstance(op, ConcatDim):
                if op.nitems_per_file:
                    nitems[op.name] = op.nitems_per_file
                else:
                    nitems[op.name] = None
        return nitems

    @property
    def concat_sequence_lens(self) -> Dict[str, Optional[int]]:
        """Dictionary mapping concat dims to sequence lengths.
        Only available if ``nitems_per_input`` is set on the dimension."""
        return {
            dim_name: (nitems * self.dims[dim_name] if nitems is not None else None)
            for dim_name, nitems in self.nitems_per_input.items()
        }

    def __getitem__(self, indexer: FilePatternIndex) -> str:
        """Get a filename path for a particular key. """
        assert len(indexer) == len(self.combine_dims)
        format_function_kwargs = {}
        for idx in indexer:
            dims = [
                dim
                for dim in self.combine_dims
                if dim.name == idx.name and dim.operation == idx.operation
            ]
            if len(dims) != 1:
                raise KeyError(r"Could not valid combine_dim for indexer {idx}")
            dim = dims[0]
            format_function_kwargs[dim.name] = dim.keys[idx.index]
        fname = self.format_function(**format_function_kwargs)
        return fname

    def __iter__(self) -> Iterator[FilePatternIndex]:
        """Iterate over all keys in the pattern. """
        for val in product(*[range(n) for n in self.shape]):
            index = Index(
                (
                    DimIndex(op.name, v, len(op.keys), op.operation)
                    for op, v in zip(self.combine_dims, val)
                )
            )
            yield index

    def items(self):
        """Iterate over key, filename pairs."""
        for key in self:
            yield key, self[key]


def pattern_from_file_sequence(file_list, concat_dim, nitems_per_file=None, **kwargs):
    """Convenience function for creating a FilePattern from a list of files."""

    keys = list(range(len(file_list)))
    concat = ConcatDim(name=concat_dim, keys=keys, nitems_per_file=nitems_per_file)

    def format_function(**kwargs):
        return file_list[kwargs[concat_dim]]

    return FilePattern(format_function, concat, **kwargs)


def prune_pattern(fp: FilePattern, nkeep: int = 2) -> FilePattern:
    """
    Create a smaller pattern from a full pattern.
    Keeps all MergeDims but only the first `nkeep` items from each ConcatDim

    :param fp: The original pattern.
    :param nkeep: The number of items to keep from each ConcatDim sequence.
    """

    new_combine_dims = []  # type: List[CombineDim]
    for cdim in fp.combine_dims:
        if isinstance(cdim, MergeDim):
            new_combine_dims.append(cdim)
        elif isinstance(cdim, ConcatDim):
            new_keys = cdim.keys[:nkeep]
            new_cdim = replace(cdim, keys=new_keys)
            new_combine_dims.append(new_cdim)
        else:  # pragma: no cover
            assert "Should never happen"

    return FilePattern(fp.format_function, *new_combine_dims)
