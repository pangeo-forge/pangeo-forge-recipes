"""
Filename / URL patterns.
"""

# This allows us to type annotate a method on a class as returning
# that class, which is otherwise impossible as that class has not
# been fully defined yet! https://peps.python.org/pep-0563/
from __future__ import annotations

import inspect
from dataclasses import dataclass, field, replace
from enum import Enum, auto
from hashlib import sha256
from itertools import product
from typing import Any, Callable, ClassVar, Dict, Iterator, List, Optional, Sequence, Tuple, Union

from .serialization import dict_drop_empty, dict_to_sha256
from .types import CombineOp, Dimension, Index, IndexedPosition, Position


@dataclass(frozen=True)
class CombineDim:
    name: str
    operation: ClassVar[CombineOp]
    keys: Sequence[Any] = field(repr=False)

    @property
    def dimension(self):
        return Dimension(self.name, self.operation)


@dataclass(frozen=True)
class ConcatDim(CombineDim):
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

    nitems_per_file: Optional[int] = None
    operation: ClassVar[CombineOp] = CombineOp.CONCAT


@dataclass(frozen=True)
class MergeDim(CombineDim):
    """Represents a merge operation across a dimension of a FilePattern.

    :param name: The name of the dimension we are are merging over. The actual
       value is not used by most recipes. The most common value is
       ``"variable"``.
    :param keys: The keys used to represent each individual item along this
      dimension. This will be used by a ``FilePattern`` object to evaluate
      the file name.
    """

    operation: ClassVar[CombineOp] = CombineOp.MERGE


def augment_index_with_start_stop(position: Position, item_lens: List[int]) -> IndexedPosition:
    """Take an index _without_ start / stop and add them based on the lens defined in sequence_lens.

    :param index: The ``DimIndex`` instance to augment.
    :param item_lens: A list of integer lengths for all items in the sequence.
    """

    if position.indexed:
        raise ValueError("This position is already indexed")
    start = sum(item_lens[: position.value])
    dimsize = sum(item_lens)
    return IndexedPosition(start, dimsize=dimsize)


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
    zarr = auto()


class FilePattern:
    """Represents an n-dimensional matrix of individual files to be combined
    through a combination of merge and concat operations. Each operation generates
    a new dimension to the matrix.

    :param format_function: A function that takes one argument for each
      combine_op and returns a string representing the filename / url paths.
      Each argument name should correspond to a ``name`` in the ``combine_dims`` list.
    :param combine_dims: A sequence of either concat or merge dimensions. The outer
      product of the keys is used to generate the full list of file paths.
    :param fsspec_open_kwargs: A dictionary of kwargs to pass to fsspec.open to aid opening
      of source files. For example, ``{"block_size": 0}`` may be passed if an HTTP source file
      server does not permit range requests. Authentication for fsspec-compatible filesystems
      may be handled here as well. For HTTP username/password-based authentication, your specific
      ``fsspec_open_kwargs`` will depend on the configuration of the source file server, but are
      likely to conform to one of the following two formats:
      ``{"username": "<username>", "password": "<password>"}``
      or ``{"auth": aiohttp.BasicAuth("<username>", "<password>")}``.
    :param query_string_secrets: If provided, these key/value pairs are appended to the query string
      of each url at runtime. Query string parameters which are not secrets should instead be
      included directly in the URLs returns by the ``format_function``.
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

    @property
    def combine_dim_keys(self) -> List[Dimension]:
        return [Dimension(dim.name, dim.operation) for dim in self.combine_dims]

    def __getitem__(self, indexer: Index) -> str:
        """Get a filename path for a particular key."""
        assert len(indexer) == len(self.combine_dims)
        format_function_kwargs = {}
        for dimension, position in indexer.items():
            dims = [
                combine_dim
                for combine_dim in self.combine_dims
                if combine_dim.dimension == dimension
            ]
            if len(dims) != 1:
                raise KeyError(f"Could not valid combine_dim for dimension {dimension}")
            dim = dims[0]
            format_function_kwargs[dim.name] = dim.keys[position.value]
        fname = self.format_function(**format_function_kwargs)
        return fname

    def __iter__(self) -> Iterator[Index]:
        """Iterate over all keys in the pattern."""
        for val in product(*[range(n) for n in self.shape]):
            index = Index(
                {
                    Dimension(op.name, op.operation): Position(v)
                    for op, v in zip(self.combine_dims, val)
                }
            )
            yield index

    def items(self):
        """Iterate over key, filename pairs."""
        for key in self:
            value = self[key]
            # for now just do something silly and bind the filename to the Position to read later
            dimension = list(key.keys())[0]
            position = list(key.values())[0]
            position.filename = value
            key = Index({dimension: position})
            yield key, value

    def sha256(self):
        """Compute a sha256 hash for the instance."""

        return self.get_merkle_list()[-1]

    def prune(self, nkeep: int = 2) -> FilePattern:
        """
        Create a smaller pattern from a full pattern.
        Keeps all MergeDims but only the first `nkeep` items from each ConcatDim

        :param nkeep: The number of items to keep from each ConcatDim sequence.
        """

        new_combine_dims = []  # type: List[CombineDim]
        for cdim in self.combine_dims:
            if isinstance(cdim, MergeDim):
                new_combine_dims.append(cdim)
            elif isinstance(cdim, ConcatDim):
                new_keys = cdim.keys[:nkeep]
                new_cdim = replace(cdim, keys=new_keys)
                new_combine_dims.append(new_cdim)
            else:  # pragma: no cover
                assert "Should never happen"

        sig = inspect.signature(self.__init__)  # type: ignore
        kwargs = {
            param: getattr(self, param)
            for param in sig.parameters.keys()
            if param not in ["format_function", "combine_dims"]
        }
        return FilePattern(self.format_function, *new_combine_dims, **kwargs)

    def get_merkle_list(self) -> List[bytes]:
        """
        Compute the merkle tree for the current FilePattern.

        Return a list of hashes, of length len(filepattern)+1. The first item in the list is
        calculated by hashing attributes of the ``FilePattern`` instance. Each subsequent
        item is calculated by hashing the byte string produced by concatinating the next
        index:filepath pair yielded by ``items()`` with the previous hash in the list.

        """

        # we exclude the format function and combine dims from ``root`` because they determine the
        # index:filepath pairs yielded by iterating over ``.items()``. if these pairs are generated
        # in a different way in the future, we ultimately don't care.
        root = {
            "fsspec_open_kwargs": self.fsspec_open_kwargs,
            "query_string_secrets": self.query_string_secrets,
            "file_type": self.file_type,
            "nitems_per_file": {
                op.name: op.nitems_per_file  # type: ignore
                for op in self.combine_dims
                if op.name in self.concat_dims
            },
        }
        # by dropping empty values from ``root``, we allow for the attributes of ``FilePattern`` to
        # change while allowing for backwards-compatibility between hashes of patterns which do not
        # set those new attributes.
        root_drop_empty = dict_drop_empty([(k, v) for k, v in root.items()])
        root_sha256 = dict_to_sha256(root_drop_empty)

        merkle_list = [root_sha256]
        for k, v in self.items():
            # key is no longer part of the hash
            value_hash = sha256(v.encode("utf-8")).digest()
            new_hash = value_hash
            new_item = sha256(merkle_list[-1] + new_hash).digest()
            merkle_list.append(new_item)

        return merkle_list

    def start_processing_from(
        self,
        old_pattern_last_hash: bytes,
    ) -> Optional[Index]:
        """Given the last hash of the merkle tree of a previous pattern, determine which (if any)
        ``Index`` key of the current pattern to begin data processing from, in order to append to
        a dataset built using the previous pattern.

        :param old_pattern_last_hash: The last hash of the merkle tree for the ``FilePattern``
            instance which was used to build the existing dataset.
        """
        for k, h in zip(self, self.get_merkle_list()):
            if h == old_pattern_last_hash:
                return k

        # No commonalities found
        return None


def pattern_from_file_sequence(
    file_list, concat_dim, nitems_per_file=None, **kwargs
) -> FilePattern:
    """Convenience function for creating a FilePattern from a list of files."""

    keys = list(range(len(file_list)))
    concat = ConcatDim(name=concat_dim, keys=keys, nitems_per_file=nitems_per_file)

    def format_function(**kwargs):
        return file_list[kwargs[concat_dim]]

    return FilePattern(format_function, concat, **kwargs)
