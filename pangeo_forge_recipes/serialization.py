from collections.abc import Collection
from dataclasses import asdict
from enum import Enum
from hashlib import sha256
from json import dumps
from typing import Any, List, Sequence

from .patterns import FilePattern, Index


def json_default(obj: Any):
    """For objects which are not serializable with ``json.dumps``, this function defines
    type-specific handlers which extract a serializable value from the object.

    :param obj: Any object which is not serializable to ``json``.
    """

    if isinstance(obj, Enum):  # custom serializer for FileType, CombineOp, etc.
        return obj.value
    elif hasattr(obj, "sha256"):
        return obj.sha256().hex()
    raise TypeError(f"object of type {type(obj).__name__} not serializable")


def dict_to_sha256(dictionary: dict) -> bytes:
    """Generates a deterministic sha256 hash for a dict by first serializing the dict to json.

    :param dictionary: The dict for which to generate a hash.
    """

    # https://death.andgravity.com/stable-hashing

    b = dumps(
        dictionary,
        default=json_default,
        ensure_ascii=False,
        sort_keys=True,
        indent=None,
        separators=(",", ":"),
    )
    return sha256(b.encode("utf-8")).digest()


def dict_drop_empty(pairs: Sequence[Sequence]) -> dict:
    """Custom factory function for ``dataclasses.asdict`` which drops fields for which the value is
    ``None`` or for which the value is an empty collection (e.g. an empty ``list`` or ``dict``).

    :param pairs: A sequence (list or tuple) of sequences of length 2, in which the first element of
      each inner sequence is a hashable which can serve as a dictionary key, and the second element
      of each inner sequence is the value to map to the key.
    """

    # https://death.andgravity.com/stable-hashing#problem-we-need-to-ignore-empty-values

    return dict((k, v) for k, v in pairs if not (v is None or not v and isinstance(v, Collection)))


def dataclass_sha256(dclass: type, ignore_keys: List[str]) -> bytes:
    """Generate a deterministic sha256 hash from a Python ``dataclass``. Fields for which the value
    is either ``None`` or an empty collection are excluded from the hash calculation automatically.
    To manually exclude other fields from the calculation, pass their names via ``igonore_keys``.
    For field values which are not json serializable, type-specific handlers are defined by the
    ``json_default`` function in this module.

    :param dclass: The dataclass for which to calculate a hash.
    :param ignore_keys: A list of field names to exclude from the hash calculation.
    """

    d = asdict(dclass, dict_factory=dict_drop_empty)
    for k in ignore_keys:
        del d[k]
    return dict_to_sha256(d)


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
        "nitems_per_file": [
            op.nitems_per_file  # type: ignore
            for op in pattern.combine_dims
            if op.name in pattern.concat_dims
        ],
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
    append to a dataset built using the previous pattern.

    :param old_pattern_last_hash: The last hash of the blockchain for the ``FilePattern`` instance
      which was used to build the existing dataset.
    :param new_pattern: A new ``FilePattern`` instance from which to append to the existing dataset.
    """

    new_chain = pattern_blockchain(new_pattern)
    for k, h in zip(new_pattern, new_chain):
        if h == old_pattern_last_hash:
            return k
