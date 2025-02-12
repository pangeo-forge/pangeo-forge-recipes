from collections.abc import Collection
from dataclasses import asdict, is_dataclass
from enum import Enum
from hashlib import sha256
from json import dumps
from typing import Any, List, Sequence


def either_encode_or_hash(obj: Any):
    """For objects which are not serializable with ``json.dumps``, this function defines
    type-specific handlers which extract either a serializable value or a hash from the object.

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
        default=either_encode_or_hash,
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

    return dict(
        (k, v)
        for k, v in pairs
        if not (v is None or not v and isinstance(v, Collection))
    )


def dataclass_sha256(dclass: Any, ignore_keys: List[str]) -> bytes:
    """Generate a deterministic sha256 hash from a Python ``dataclass``. Fields for which the value
    is either ``None`` or an empty collection are excluded from the hash calculation automatically.
    To manually exclude other fields from the calculation, pass their names via ``igonore_keys``.
    For field values which are not json serializable, type-specific handlers are defined by the
    ``json_default`` function in this module.

    :param dclass: The dataclass for which to calculate a hash.
    :param ignore_keys: A list of field names to exclude from the hash calculation.
    """
    if not is_dataclass(dclass):
        raise ValueError("dclass must be an instance of a dataclass")

    d = asdict(dclass, dict_factory=dict_drop_empty)
    for k in ignore_keys:
        del d[k]
    return dict_to_sha256(d)
