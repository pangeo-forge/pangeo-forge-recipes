from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np


class DatasetMergeError(Exception):
    pass


def empty_xarray_schema():
    return {"attrs": None, "coords": None, "data_vars": None, "dims": None}


@dataclass
class XarrayConcatAccumulator:
    concat_dim: str
    schema: dict = field(default_factory=empty_xarray_schema)
    chunks: list[tuple[int, int]] = field(default_factory=list)

    def add_input(self, s: dict, position: int) -> None:
        self.schema = _merge_xarray_schemas(self.schema, s, concat_dim=self.concat_dim)
        self.chunks.append((position, s["dims"][self.concat_dim]))

    def __add__(self, other: XarrayConcatAccumulator) -> XarrayConcatAccumulator:
        print(f"called __add__ {self}, {other}")
        if other.concat_dim != self.concat_dim:
            raise DatasetMergeError("Can't merge accumulators with different concat_dims")
        new_schema = _merge_xarray_schemas(self.schema, other.schema, self.concat_dim)
        assert self.concat_dim == other.concat_dim
        new_chunks = self.chunks + other.chunks
        return XarrayConcatAccumulator(self.concat_dim, new_schema, new_chunks)

    @property
    def chunk_lens(self) -> tuple[int, ...]:
        if len(self.chunks) == 0:
            return ()
        expected_dim_len = self.schema["dims"][self.concat_dim]
        sorted_chunks = sorted(self.chunks)
        keys = [item[0] for item in sorted_chunks]
        if not keys == list(range(len(keys))):
            raise DatasetMergeError(f"Did not get the expected chunk keys: {keys}")
        chunk_lens = [item[1] for item in sorted_chunks]
        actual_dim_len = sum(chunk_lens)
        if not actual_dim_len == expected_dim_len:
            raise DatasetMergeError(
                f"Actual dim len {actual_dim_len} doesn't match schema dim len {expected_dim_len}"
            )
        return tuple(chunk_lens)


def _merge_dims(d1, d2, concat_dim):
    if d1 is None:
        return d2
    all_dims = set(d1) | set(d2)
    new_dims = {}
    for dim in all_dims:
        l1 = d1.get(dim, 0)
        l2 = d2.get(dim, 0)
        if dim == concat_dim:
            dim_len = l1 + l2
        elif l1 != l2:
            raise DatasetMergeError(f"Dimensions for {dim} have different sizes: {l1}, {l2}")
        else:
            dim_len = l1
        new_dims[dim] = dim_len
    return new_dims


def _merge_attrs(a1, a2):
    if a1 is None:
        return a2
    # for now, only keep attrs that are the same in both datasets
    common_attrs = set(a1) & set(a2)
    new_attrs = {}
    for key in common_attrs:
        if a1[key] == a2[key]:
            new_attrs[key] = a1[key]
    return new_attrs


def _merge_dtype(d1, d2):
    return str(np.promote_types(d1, d2))


def _merge_vars(v1, v2, concat_dim, allow_both=False):
    if v1 is None:
        return v2
    all_vars = set(v1) | set(v2)
    new_vars = {}
    for vname in all_vars:
        if vname not in v1:
            new_vars[vname] = v2[vname]
        elif vname not in v2:
            new_vars[vname] = v1[vname]
        else:
            if concat_dim is None and not allow_both:
                raise DatasetMergeError(f"Can't merge datasets with the same variable {vname}")
            attrs = _merge_attrs(v1[vname]["attrs"], v2[vname]["attrs"])
            dtype = _merge_dtype(v1[vname]["dtype"], v2[vname]["dtype"])
            (d1, s1), (d2, s2) = (
                (v1[vname]["dims"], v1[vname]["shape"]),
                (v2[vname]["dims"], v2[vname]["shape"]),
            )
            if d1 != d2:
                # should we make this logic insensitive to permutations?
                raise DatasetMergeError(f"Can't merge variables with different dims {d1}, {d2}")
            dims = d1
            shape = []
            for dname, l1, l2 in zip(dims, s1, s2):
                if dname == concat_dim:
                    shape.append(l1 + l2)
                elif l1 != l2:
                    raise DatasetMergeError(
                        f"Can't merge variables with different shapes {s1}, {s2}"
                    )
                else:
                    shape.append(l1)
            new_vars[vname] = {"dims": dims, "attrs": attrs, "dtype": dtype, "shape": tuple(shape)}
    return new_vars


def _merge_xarray_schemas(s1: dict, s2: dict, concat_dim: Optional[str] = None):
    dims = _merge_dims(s1["dims"], s2["dims"], concat_dim)
    attrs = _merge_attrs(s1["attrs"], s2["attrs"])
    data_vars = _merge_vars(s1["data_vars"], s2["data_vars"], concat_dim)
    coords = _merge_vars(s1["coords"], s2["coords"], concat_dim, allow_both=True)
    return {"attrs": attrs, "coords": coords, "data_vars": data_vars, "dims": dims}
