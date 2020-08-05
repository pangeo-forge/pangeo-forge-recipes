# `pangeo_forge.pipelines`

A collection of Pangeo-forge _Pipelines_.

## Details

- An abstract Pipeline class is defined in `base.py`.
- A general file naming convention might be `{source}_{container}_{target}.py`. For example, a pipeline that downloads netCDF files from a remote http server and publishes a Zarr store would be `http_xarray_zarr.py`. The main class in each module would follow a similar convention, e.g. `HttpXarrayZarr`.
