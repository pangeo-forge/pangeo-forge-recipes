# Recipe Tutorials

These tutorials are deep dives into how to develop and debug Pangeo Forge recipes.

## Zarr Recipes
```{toctree}
:maxdepth: 1

xarray_zarr/netcdf_zarr_sequential
xarray_zarr/cmip6-recipe
xarray_zarr/multi_variable_recipe
xarray_zarr/terraclimate
xarray_zarr/opendap_subset_recipe
```

## Reference Recipes


Unlike the standard Zarr recipes, these reference recipes utilize [kerchunk](https://fsspec.github.io/kerchunk/). The difference between these two methods is that the standard Zarr recipe creates a copy of the original dataset in the Zarr format, while the kerchunk-based reference recipe does not copy the data and instead creates a Kerchunk mapping, which allows legacy formats such as (NetCDF, GRIB2, TIFF, FITS etc.) to be read as if they were Zarr datasets. More details about how Kerchunk works can be found in the [kerchunk docs](https://fsspec.github.io/kerchunk/detail.html). Kerchunk can be used independently outside of Pangeo-Forge. In these examples, Pangeo-Forge acts as a pipeline for generating Kerchunk references.
### When to use Reference Recipes

When choosing which recipe object you want to use, it is important to consider the end use cases of your dataset as both recipe classes have pros and cons.

If the original data are not already in the cloud, there may be little benefit to using a reference recipe, because the access time for the data could be very slow. However, it may still be useful for data stored at on-premises data centers, with semi-cloud like access.

A reference recipe does not copy the original data, but instead produces and stores reference sets which are much smaller. They also do not need to download and read all the bytes of the original files. However, modification of the underlying data is not possible, for example, the chunking schema of a dataset cannot be modified with Kerchunk, so you are limited to the initial chunk schema of the dataset. If you need to optimize your datasets chunking schema for space or time, the standard Zarr recipe is the only option. While you cannot modify the underlying chunk schema, changes in the metadata (attributes, encoding, etc.) can be applied.




```{toctree}
:maxdepth: 1

hdf_reference/reference_cmip6
```
