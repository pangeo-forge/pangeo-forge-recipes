# Recipe Tutorials

These tutorials are deep dives into how to develop and debug Pangeo Forge recipes.

## XarrayZarrRecipe

These tutorials all use the {class}`pangeo_forge_recipes.recipes.XarrayZarrRecipe` class.

```{toctree}
:maxdepth: 1

xarray_zarr/netcdf_zarr_sequential
xarray_zarr/cmip6-recipe
xarray_zarr/multi_variable_recipe
xarray_zarr/terraclimate
xarray_zarr/opendap_subset_recipe
```

## HDFReferenceRecipe

These tutorials all use the {class}`pangeo_forge_recipes.recipes.HDFReferenceRecipe` class:

Unlike the standard `XarrayZarrRecipe` recipe creation method, these reference recipes utilize [kerchunk](https://fsspec.github.io/kerchunk/) to create a ReferenceRecipe. The difference between these two methods is that the standard XarrayZarrRecipe creates a copy of the original dataset in the Zarr format, while the kerchunk-based ReferenceRecipe does not copy the data and instead creates a kerchunk mapping, which allows legacy formats such as (NetCDF, GRIB2, TIFF, FITS etc.) to be read as if they were Zarr stores. More details about how kerchunk works can be found in the [kerchunk docs](https://fsspec.github.io/kerchunk/detail.html). Kerchunk can be used independently outside of Pangeo-Forge. In these examples, Pangeo-Forge acts as a runner for kerchunk. Currently only HDF/NetCDF Reference Recipes are supported by Pangeo-Forge.

### When to use Reference Recipes

When choosing which recipe object you want to use, it is important to consider the end use cases of your dataset as both recipe classes have pros and cons.

In general, ReferenceRecipes store and copy much less data overall. However, modification of the underlying data is more limited, for example, the chunking schema of a dataset can not be modified with kerchunk, so you are dependent on the initial chunk schema of the dataset. If you need to optimize your datasets chunking schema for space or time, the standard `XarrayZarrRecipe` might be a better bet.



```{toctree}
:maxdepth: 1

hdf_reference/reference_cmip6
```
