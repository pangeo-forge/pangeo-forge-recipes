import cftime
import pytest
import xarray as xr

from pangeo_forge_recipes.aggregation import (
    dataset_to_schema,
    determine_target_chunks,
    schema_to_template_ds,
    schema_to_zarr,
)
from pangeo_forge_recipes.storage import FSSpecTarget

from .data_generation import make_ds


def _expected_chunks(shape, chunk):
    if chunk is None:
        return (shape,)
    base = (shape // chunk) * (chunk,)
    extra = ((shape % chunk),) if (shape % chunk) else tuple()
    return base + extra


@pytest.mark.parametrize("specified_chunks", [{}, {"time": 1}, {"time": 2}, {"time": 2, "lon": 9}])
def test_schema_to_template_ds(specified_chunks):
    nt = 3
    ds = make_ds(nt=nt)
    schema = dataset_to_schema(ds)
    dst = schema_to_template_ds(schema, specified_chunks=specified_chunks)
    for v in dst:
        var = dst[v]
        for dim in var.dims:
            size = var.sizes[dim]
            chunksize = var.chunksizes[dim]
            expected_chunksize = _expected_chunks(size, specified_chunks.get(dim, None))
            assert chunksize == expected_chunksize
    # Confirm original time units have been preserved
    assert ds.time.encoding.get("units") == dst.time.encoding.get("units")
    schema2 = dataset_to_schema(dst)
    assert schema == schema2


@pytest.mark.parametrize(
    "specified_chunks",
    [{}, {"time": 1}, {"time": 2}, {"time": 2, "lon": 9}, {"time": 3}, {"time": 3, "lon": 7}],
)
@pytest.mark.parametrize("include_all_dims", [True, False])
def test_determine_target_chunks(specified_chunks, include_all_dims):
    nt = 3
    ds = make_ds(nt=nt)
    schema = dataset_to_schema(ds)

    chunks = determine_target_chunks(schema, specified_chunks, include_all_dims)

    if include_all_dims:
        for name, default_chunk in schema["dims"].items():
            assert name in chunks
            if name in specified_chunks:
                assert chunks[name] == specified_chunks[name]
            else:
                assert chunks[name] == default_chunk
    else:
        for name, cs in specified_chunks.items():
            if name in chunks and name in schema["dims"]:
                assert chunks[name] != schema["dims"][name]


def test_schema_to_template_ds_cftime():
    ds = xr.decode_cf(
        xr.DataArray(
            [1],
            dims=["time"],
            coords={
                "time": (
                    "time",
                    [1],
                    {"units": "days since 1850-01-01 00:00:00", "calendar": "noleap"},
                )
            },
        ).to_dataset(name="tas")
    )
    schema = dataset_to_schema(ds)
    dst = schema_to_template_ds(schema)
    assert ds.time.encoding.get("units") == dst.time.encoding.get("units")
    assert isinstance(dst.time.values[0], cftime.datetime)


def test_schema_to_template_ds_attrs():

    attrs = {"test_attr_key": "test_attr_value"}
    ds = xr.decode_cf(
        xr.DataArray(
            [1],
            dims=["time"],
            coords={
                "time": (
                    "time",
                    [1],
                    {"units": "days since 1850-01-01 00:00:00", "calendar": "noleap"},
                )
            },
            attrs={"original_attrs_key": "original_attrs_value"},
        ).to_dataset(name="tas", promote_attrs=True)
    )

    schema = dataset_to_schema(ds)
    dst = schema_to_template_ds(schema, attrs=attrs)

    assert dst.attrs["pangeo-forge:test_attr_key"] == "test_attr_value"
    assert dst.attrs["original_attrs_key"] == "original_attrs_value"
