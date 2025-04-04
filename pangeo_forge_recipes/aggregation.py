from __future__ import annotations

from typing import Dict, Optional, TypedDict

import cftime
import dask.array as dsa
import numpy as np
import xarray as xr
import zarr


class XarraySchema(TypedDict):
    attrs: Dict
    coords: Dict
    data_vars: Dict
    dims: Dict
    chunks: Dict


def dataset_to_schema(ds: xr.Dataset) -> XarraySchema:
    """Convert the output of `dataset.to_dict(data=False, encoding=True)` to a schema
    (Basically just adds chunks, which is not part of the Xarray output).
    """

    # Remove redundant encoding options
    for v in ds.variables:
        for option in ["source"]:
            if option in ds[v].encoding:
                del ds[v].encoding[option]
    d = ds.to_dict(data=False, encoding=True)
    return XarraySchema(
        attrs=d.get("attrs"),
        coords=d.get("coords"),
        data_vars=d.get("data_vars"),
        dims=d.get("dims"),
        chunks=d.get("chunks", {}),
    )


def _combine_xarray_schemas(
    s1: Optional[XarraySchema], s2: Optional[XarraySchema], concat_dim: Optional[str] = None
) -> XarraySchema:
    if s1 is None and s2 is None:
        raise ValueError(
            "Encountered two empty XarraySchemas during combine: one must be non-empty"
        )
    if s1 is None:
        assert s2 is not None
        return s2
    elif s2 is None:
        assert s1 is not None
        return s1
    else:
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
    if not d1:
        return d2
    all_dims = set(d1) | set(d2)
    new_dims = {}
    for dim in all_dims:
        l1 = d1.get(dim, 0)
        l2 = d2.get(dim, 0)
        if dim == concat_dim:
            dim_len = l1 + l2
        elif l1 != l2:
            raise ValueError(f"Dimensions for {dim} have different sizes: {l1}, {l2}")
        else:
            dim_len = l1
        new_dims[dim] = dim_len
    return new_dims


ChunkDict = Dict[str, Dict[int, int]]
# chunks is a dict like
# {"lon": {0: 5, 1: 5}}
#  dim_name: {position, chunk_len}


def _combine_chunks(c1: ChunkDict, c2: ChunkDict, concat_dim: Optional[str]) -> ChunkDict:
    if not c1:
        return c2

    chunks = {}
    if set(c1) != set(c2):
        raise ValueError("Expect the same dims in both chunk sets")
    for dim in c1:
        if dim == concat_dim:
            # merge chunks
            # check for overlapping keys
            if set(c1[dim]) & set(c2[dim]):
                raise ValueError("Found overlapping keys in concat_dim")
            chunks[dim] = {**c1[dim], **c2[dim]}
        else:
            if c1[dim] != c2[dim]:
                raise ValueError("Non concat_dim chunks must be the same")
            chunks[dim] = c1[dim]
    return chunks


def _combine_attrs(a1: dict, a2: dict) -> dict:
    if not a1:
        return a2
    # for now, only keep attrs that are the same in both datasets
    common_attrs = set(a1) & set(a2)
    new_attrs = {}
    for key in common_attrs:
        # treat NaNs as equal in the attrs
        if (
            isinstance(a1[key], np.floating)
            and isinstance(a2[key], np.floating)
            and np.isnan(a1[key])
            and np.isnan(a2[key])
        ):
            new_attrs[key] = a1[key]
        elif a1[key] == a2[key]:
            new_attrs[key] = a1[key]
    return new_attrs


def _combine_dtype(d1, d2):
    return str(np.promote_types(d1, d2))


def _combine_vars(v1, v2, concat_dim, allow_both=False):
    if not v1:
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
                raise ValueError(f"Can't merge datasets with the same variable {vname}")
            attrs = _combine_attrs(v1[vname]["attrs"], v2[vname]["attrs"])
            dtype = _combine_dtype(v1[vname]["dtype"], v2[vname]["dtype"])
            # Can combine encoding using the same approach as attrs
            encoding = _combine_attrs(v1[vname]["encoding"], v2[vname]["encoding"])
            (d1, s1), (d2, s2) = (
                (v1[vname]["dims"], v1[vname]["shape"]),
                (v2[vname]["dims"], v2[vname]["shape"]),
            )
            if d1 != d2:
                # should we make this logic insensitive to permutations?
                raise ValueError(f"Can't merge variables with different dims {d1}, {d2}")
            dims = d1
            shape = []
            for dname, l1, l2 in zip(dims, s1, s2):
                if dname == concat_dim:
                    shape.append(l1 + l2)
                elif l1 != l2:
                    raise ValueError(f"Can't merge variables with different shapes {s1}, {s2}")
                else:
                    shape.append(l1)
            new_vars[vname] = {
                "dims": dims,
                "attrs": attrs,
                "dtype": dtype,
                "shape": tuple(shape),
                "encoding": encoding,
            }

    return new_vars


def _to_variable(template, target_chunks):
    """Create an xarray variable from a specification."""
    dims = template["dims"]
    shape = template["shape"]
    # todo: possibly override with encoding dtype once we add encoding to the schema
    dtype = template["dtype"]
    chunks = tuple(target_chunks[dim] for dim in dims)

    encoding = template.get("encoding", {})

    # special case for cftime object dtypes
    if dtype == "object" and "calendar" in encoding and "units" in encoding:
        value = cftime.num2date(0, units=encoding["units"], calendar=encoding["calendar"])
        data = dsa.full(shape, value, chunks=chunks)
    else:
        # we pick zeros as the safest value to initialize empty data with
        # will only be used for dimension coordinates
        data = dsa.zeros(shape=shape, chunks=chunks, dtype=dtype)

    # TODO: add more encoding
    encoding["chunks"] = chunks
    return xr.Variable(dims=dims, data=data, attrs=template["attrs"], encoding=encoding)


def determine_target_chunks(
    schema: XarraySchema,
    specified_chunks: Optional[Dict[str, int]] = None,
    include_all_dims: bool = True,
) -> Dict[str, int]:
    # if the schema is chunked, use that
    target_chunks = {dim: dimchunks[0] for dim, dimchunks in schema["chunks"].items()}
    for dim, dimsize in schema["dims"].items():
        if dim not in target_chunks:
            target_chunks[dim] = dimsize
    # override with any specified chunks
    target_chunks.update(specified_chunks or {})
    if not include_all_dims:
        # remove chunks with the same size as their dimension
        dims_to_remove = [dim for dim, cs in target_chunks.items() if cs == schema["dims"][dim]]
        for dim in dims_to_remove:
            del target_chunks[dim]
    return target_chunks


def schema_to_template_ds(
    schema: XarraySchema,
    specified_chunks: Optional[Dict[str, int]] = None,
    attrs: Optional[Dict[str, str]] = None,
) -> xr.Dataset:
    """Convert a schema to an xarray dataset as lazily as possible."""

    target_chunks = determine_target_chunks(schema, specified_chunks)

    data_vars = {
        name: _to_variable(template, target_chunks)
        for name, template in schema["data_vars"].items()
    }

    coords = {
        name: _to_variable(template, target_chunks) for name, template in schema["coords"].items()
    }
    dataset_attrs = schema["attrs"]

    if attrs and isinstance(attrs, dict):
        for k, v in attrs.items():  # type: ignore
            dataset_attrs[f"pangeo-forge:{k}"] = v

    ds = xr.Dataset(data_vars=data_vars, coords=coords, attrs=dataset_attrs)
    return ds


def schema_to_zarr(
    schema: XarraySchema,
    target_store: zarr.storage.FsspecStore,
    target_chunks: Optional[Dict[str, int]] = None,
    attrs: Optional[Dict[str, str]] = None,
    consolidated_metadata: Optional[bool] = True,
    encoding: Optional[Dict] = None,
    append_dim: Optional[str] = None,
) -> zarr.storage.FsspecStore:
    """Initialize a zarr group based on a schema."""
    if append_dim:
        # if appending, only keep schema for coordinate to append. if we don't drop other
        # coords, we may end up overwriting existing data on the `ds.to_zarr` call below.
        schema["coords"] = {k: v for k, v in schema["coords"].items() if k == append_dim}
    ds = schema_to_template_ds(schema, specified_chunks=target_chunks, attrs=attrs)
    # using mode="w" makes this function idempotent when not appending

    ds.to_zarr(
        target_store,
        append_dim=append_dim,
        mode=("a" if append_dim else "w"),
        compute=False,
        zarr_format=3,  # TODO: We force Zarr format 3 here, we should address
        consolidated=False,
        encoding=encoding,
    )

    return target_store
