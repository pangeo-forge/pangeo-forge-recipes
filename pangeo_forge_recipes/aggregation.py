from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

import dask.array as dsa
import numpy as np
import xarray as xr
import zarr


class DatasetCombineError(Exception):
    pass


def empty_xarray_schema():
    return {"attrs": None, "coords": None, "data_vars": None, "dims": None, "chunks": None}


@dataclass
class XarrayCombineAccumulator:
    """An object used to help combine Xarray schemas.

    :param schema: A schema to initialize the accumulator with.
    :param concat_dim: If set, this accumulator applies concat rules.
       Otherwise applies merge rules.
    """

    schema: dict = field(default_factory=empty_xarray_schema)
    concat_dim: Optional[str] = None

    def add_input(self, s: dict, position: int) -> None:
        s = s.copy()  # avoid modifying input
        if "chunks" not in s:
            # By default, the output of ds.to_dict(data=False) includes nothing
            # about the chunks. We may want an intermediary function that actually
            # looks at the contents of the dataset and sets chunks appropriately.
            s["chunks"] = {}
        else:
            s["chunks"] = s["chunks"].copy()
        if self.concat_dim:
            assert (
                self.concat_dim not in s["chunks"]
            ), "Concat dim should be unchunked for new input"
            s["chunks"][self.concat_dim] = {position: s["dims"][self.concat_dim]}
        self.schema = _combine_xarray_schemas(self.schema, s, concat_dim=self.concat_dim)

    def __add__(self, other: XarrayCombineAccumulator) -> XarrayCombineAccumulator:
        if other.concat_dim != self.concat_dim:
            raise DatasetCombineError("Can't merge accumulators with different concat_dims")
        new_schema = _combine_xarray_schemas(self.schema, other.schema, self.concat_dim)
        return XarrayCombineAccumulator(new_schema, self.concat_dim)


def _combine_xarray_schemas(s1: dict, s2: dict, concat_dim: Optional[str] = None):
    dims = _combine_dims(s1["dims"], s2["dims"], concat_dim)
    chunks = _combine_chunks(s1["chunks"], s2["chunks"], concat_dim)
    attrs = _combine_attrs(s1["attrs"], s2["attrs"])
    data_vars = _combine_vars(s1["data_vars"], s2["data_vars"], concat_dim)
    coords = _combine_vars(s1["coords"], s2["coords"], concat_dim, allow_both=True)
    return {
        "attrs": attrs,
        "coords": coords,
        "data_vars": data_vars,
        "dims": dims,
        "chunks": chunks,
    }


def _combine_dims(
    d1: Dict[str, int], d2: Dict[str, int], concat_dim: Optional[str]
) -> Dict[str, int]:
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
            raise DatasetCombineError(f"Dimensions for {dim} have different sizes: {l1}, {l2}")
        else:
            dim_len = l1
        new_dims[dim] = dim_len
    return new_dims


ChunkDict = Dict[str, Dict[int, int]]
# chunks is a dict like
# {"lon": {0: 5, 1: 5}}
#  dim_name: {position, chunk_len}


def _combine_chunks(c1: ChunkDict, c2: ChunkDict, concat_dim: Optional[str]) -> ChunkDict:
    if c1 is None:
        return c2

    chunks = {}
    if set(c1) != set(c2):
        raise DatasetCombineError("Expect the same dims in both chunk sets")
    for dim in c1:
        if dim == concat_dim:
            # merge chunks
            # check for overlapping keys
            if set(c1[dim]) & set(c2[dim]):
                raise DatasetCombineError("Found overlapping keys in concat_dim")
            chunks[dim] = {**c1[dim], **c2[dim]}
        else:
            if c1[dim] != c2[dim]:
                raise DatasetCombineError("Non concat_dim chunks must be the same")
            chunks[dim] = c1[dim]
    return chunks


def _combine_attrs(a1: dict, a2: dict) -> dict:
    if a1 is None:
        return a2
    # for now, only keep attrs that are the same in both datasets
    common_attrs = set(a1) & set(a2)
    new_attrs = {}
    for key in common_attrs:
        if a1[key] == a2[key]:
            new_attrs[key] = a1[key]
    return new_attrs


def _combine_dtype(d1, d2):
    return str(np.promote_types(d1, d2))


def _combine_vars(v1, v2, concat_dim, allow_both=False):
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
                raise DatasetCombineError(f"Can't merge datasets with the same variable {vname}")
            attrs = _combine_attrs(v1[vname]["attrs"], v2[vname]["attrs"])
            dtype = _combine_dtype(v1[vname]["dtype"], v2[vname]["dtype"])
            (d1, s1), (d2, s2) = (
                (v1[vname]["dims"], v1[vname]["shape"]),
                (v2[vname]["dims"], v2[vname]["shape"]),
            )
            if d1 != d2:
                # should we make this logic insensitive to permutations?
                raise DatasetCombineError(f"Can't merge variables with different dims {d1}, {d2}")
            dims = d1
            shape = []
            for dname, l1, l2 in zip(dims, s1, s2):
                if dname == concat_dim:
                    shape.append(l1 + l2)
                elif l1 != l2:
                    raise DatasetCombineError(
                        f"Can't merge variables with different shapes {s1}, {s2}"
                    )
                else:
                    shape.append(l1)
            new_vars[vname] = {"dims": dims, "attrs": attrs, "dtype": dtype, "shape": tuple(shape)}
    return new_vars


def _to_variable(template, target_chunks):
    """Create an xarray variable from a specification."""
    dims = template["dims"]
    shape = template["shape"]
    # todo: possibly override with encoding dtype once we add encoding to the schema
    dtype = template["dtype"]
    chunks = tuple(target_chunks[dim] for dim in dims)
    # we pick ones as the safest value to initialize empty data with
    # will only be used for dimension coordinates
    data = dsa.ones(shape=shape, chunks=chunks, dtype=dtype)
    # TODO: add encoding
    return xr.Variable(dims=dims, data=data, attrs=template["attrs"])


def schema_to_template_ds(
    schema: Dict, specified_chunks: Optional[Dict[str, int]] = None
) -> xr.Dataset:
    """Convert a schema to an xarray dataset as lazily as possible."""

    # start by defaulting to the full shape for chunks
    target_chunks = schema["dims"].copy()
    # if the schema is chunked, use that
    target_chunks.update({dim: dimchunks[0] for dim, dimchunks in schema["chunks"].items()})
    # finally override with any specified chunks
    target_chunks.update(specified_chunks or {})

    data_vars = {
        name: _to_variable(template, target_chunks)
        for name, template in schema["data_vars"].items()
    }

    coords = {
        name: _to_variable(template, target_chunks) for name, template in schema["coords"].items()
    }

    ds = xr.Dataset(data_vars=data_vars, coords=coords, attrs=schema["attrs"])
    return ds


def schema_to_zarr(
    schema: Dict,
    target_store: zarr.storage.FSStore,
    target_chunks: Optional[Dict[str, int]] = None,
) -> zarr.storage.FSStore:
    """Initialize a zarr group based on a schema."""
    ds = schema_to_template_ds(schema, specified_chunks=target_chunks)
    # using mode="w" makes this function idempotent
    ds.to_zarr(target_store, mode="w")
    return target_store
