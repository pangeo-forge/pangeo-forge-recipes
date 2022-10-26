"""
Filename / URL patterns.
"""
import inspect
from dataclasses import dataclass, field, replace
from enum import Enum, auto
from hashlib import sha256
from itertools import product
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    FrozenSet,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from .serialization import dataclass_sha256, dict_drop_empty, dict_to_sha256


class CombineOp(Enum):
    """Used to uniquely identify different combine operations across Pangeo Forge Recipes."""

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


@dataclass(frozen=True, order=True)
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


class Index(FrozenSet[DimIndex]):
    pass


CombineDim = Union[MergeDim, ConcatDim]
FilePatternIndex = Index


class AutoName(Enum):
    # Recommended by official Python docs for auto naming:
    # https://docs.python.org/3/library/enum.html#using-automatic-values
    def _generate_next_value_(name, start, count, last_values):
        return name


class FileType(AutoName):
    unknown = auto()
    netcdf3 = auto()
    netcdf4 = auto()
    grib = auto()
    opendap = auto()
    tiff = auto()


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
    :param file_type: The file format of the source files for this pattern. Must be one of
      the options defined by ``pangeo_forge_recipes.patterns.FileType``.
      Note: ``FileType.opendap`` cannot be used with caching.
    """

    def __init__(
        self,
        format_function: Callable,
        *combine_dims: CombineDim,
        fsspec_open_kwargs: Optional[Dict[str, Any]] = None,
        query_string_secrets: Optional[Dict[str, str]] = None,
        file_type: str = "netcdf4",
    ):
        self.format_function = format_function
        self.combine_dims = combine_dims
        self.fsspec_open_kwargs = fsspec_open_kwargs if fsspec_open_kwargs else {}
        self.query_string_secrets = query_string_secrets if query_string_secrets else {}
        self.file_type = FileType(file_type)

        if self.fsspec_open_kwargs and self.file_type == FileType.opendap:
            raise ValueError(
                "OPeNDAP inputs are not opened with `fsspec`. "
                "When passing `fsspec_open_kwargs`, `file_type` cannot be `opendap`."
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
        """Get a filename path for a particular key."""
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
        """Iterate over all keys in the pattern."""
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

    @property
    def sha256(self):
        """Compute a sha256 hash for the instance."""

        return pattern_blockchain(self).pop(-1)


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

    sig = inspect.signature(fp.__init__)  # type: ignore
    kwargs = {
        param: getattr(fp, param)
        for param in sig.parameters.keys()
        if param not in ["format_function", "combine_dims"]
    }
    return FilePattern(fp.format_function, *new_combine_dims, **kwargs)


def pattern_blockchain(pattern: FilePattern) -> List[bytes]:
    """For a ``FilePattern`` instance, compute a blockchain, i.e. a list of hashes of length N+1,
    where N is the number of index:filepath pairs yielded by the ``FilePattern`` instance's
    ``.items()`` method. The first item in the list is calculated by hashing instance attributes.
    Each subsequent item is calculated by hashing the byte string produced by concatenating the next
    index:filepath pair yielded by ``.items()`` with the previous hash in the list.

    :param pattern: The ``FilePattern`` instance for which to calculate a blockchain.
    """

    # we exclude the format function and combine dims from ``root`` because they determine the
    # index:filepath pairs yielded by iterating over ``.items()``. if these pairs are generated in
    # a different way in the future, we ultimately don't care.
    root = {
        "fsspec_open_kwargs": pattern.fsspec_open_kwargs,
        "query_string_secrets": pattern.query_string_secrets,
        "file_type": pattern.file_type,
        "nitems_per_file": {
            op.name: op.nitems_per_file  # type: ignore
            for op in pattern.combine_dims
            if op.name in pattern.concat_dims
        },
    }
    # by dropping empty values from ``root``, we allow for the attributes of ``FilePattern`` to
    # change while allowing for backwards-compatibility between hashes of patterns which do not
    # set those new attributes.
    root_drop_empty = dict_drop_empty([(k, v) for k, v in root.items()])
    root_sha256 = dict_to_sha256(root_drop_empty)

    blockchain = [root_sha256]
    for k, v in pattern.items():
        key_hash = b"".join(
            sorted([dataclass_sha256(dimindex, ignore_keys=["sequence_len"]) for dimindex in k])
        )
        value_hash = sha256(v.encode("utf-8")).digest()
        new_hash = key_hash + value_hash
        new_block = sha256(blockchain[-1] + new_hash).digest()
        blockchain.append(new_block)

    return blockchain


def match_pattern_blockchain(  # type: ignore
    old_pattern_last_hash: bytes,
    new_pattern: FilePattern,
) -> Index:
    """Given the last hash of the blockchain for a previous pattern, and a new pattern, determine
    which (if any) ``Index`` key of the new pattern to begin processing data from, in order to
    append to a dataset build using the previous pattern.

    :param old_pattern_last_hash: The last hash of the blockchain for the ``FilePattern`` instance
      which was used to build the existing dataset.
    :param new_pattern: A new ``FilePattern`` instance from which to append to the existing dataset.
    """

    new_chain = pattern_blockchain(new_pattern)
    for k, h in zip(new_pattern, new_chain):
        if h == old_pattern_last_hash:
            return k
