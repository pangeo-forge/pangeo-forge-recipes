import fsspec
import xarray as xr
from prefect import Flow, Task

import pangeo_forge.tasks.xarray


def test_combine_and_write():
    ds = xr.tutorial.open_dataset("rasm").load()
    dsets = ds.isel(time=slice(18)), ds.isel(time=slice(18, None))
    fs = fsspec.get_filesystem_class("memory")()

    for i, dset in enumerate(dsets):
        as_bytes = dset.to_netcdf()

        with fs.open(f"cache/{i}.nc", "wb") as f:
            f.write(as_bytes)

    sources = [f"memory://{dset}" for dset in fs.ls("cache")]

    # In a flow context

    target = "memory://target.zarr"
    with Flow("test") as flow:
        result = pangeo_forge.tasks.xarray.combine_and_write(sources, target, concat_dim="time")
        assert isinstance(result, Task)
    flow.validate()

    result = pangeo_forge.tasks.xarray.combine_and_write.run(sources, target, concat_dim="time")
    assert result == target
    result = xr.open_zarr(fs.get_mapper("target.zarr"))
    xr.testing.assert_equal(ds, result)
