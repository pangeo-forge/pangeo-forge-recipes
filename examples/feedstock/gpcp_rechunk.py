# Example recipe to demonstate reading from an existing Zarr store and
# writing a new Zarr store with a differant chunking structure


import apache_beam as beam
import zarr

from pangeo_forge_recipes.patterns import FileType, pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    ConsolidateDimensionCoordinates,
    ConsolidateMetadata,
    OpenWithXarray,
    StoreToZarr,
)

pattern = pattern_from_file_sequence(
    ["https://ncsa.osn.xsede.org/Pangeo/pangeo-forge/gpcp-feedstock/gpcp.zarr"],
    concat_dim="time",
)


def test_ds(store: zarr.storage.FsspecStore) -> zarr.storage.FsspecStore:
    import xarray as xr

    assert xr.open_dataset(store, engine="zarr", chunks={})
    return store


recipe = (
    beam.Create(pattern.items())
    | OpenWithXarray(file_type=FileType("zarr"), xarray_open_kwargs={"chunks": {}})
    | StoreToZarr(
        store_name="gpcp_rechunked.zarr",
        target_chunks={"time": 9226, "latitude": 16, "longitude": 36, "nv": 2},
        combine_dims=pattern.combine_dim_keys,
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | "Test dataset" >> beam.Map(test_ds)
)
