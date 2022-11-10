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

## ReferenceRecipes

These tutorials all use the {class}`pangeo_forge_recipes.recipes.ReferenceRecipe`.

Note: While kerchunk currently supports [netcdf/hdf5, grib2, tiff and fits], the {class}`pangeo_forge_recipes.recipes.ReferenceRecipe` only has implimentations for [netcdf/hdf5 and grib2].

```{toctree}
:maxdepth: 1

hdf_reference/reference_cmip6
grib_reference/reference_HRRR
```
