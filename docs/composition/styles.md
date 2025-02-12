# Common styles

## Open with Xarray, write to Zarr

This recipe category uses [Xarray](http://xarray.pydata.org/) to open input files and
[Zarr](https://zarr.readthedocs.io/) as the target dataset format. Inputs can be in any
[file format Xarray can read](http://xarray.pydata.org/en/latest/user-guide/io.html),
including NetCDF, OPeNDAP, GRIB, Zarr, and, via [rasterio](https://rasterio.readthedocs.io/),
GeoTIFF and other geospatial raster formats. The target Zarr dataset will conform to the
[Xarray Zarr encoding conventions](http://xarray.pydata.org/en/latest/internals/zarr-encoding-spec.html).

```{tip}
The following example recipes are representative of this style:

- {doc}`examples/gpcp-from-gcs`
- {doc}`examples/noaa-oisst`
```

Below we give a very basic overview of how this recipe is used.

First you must define a {doc}`file pattern <file_patterns>`.
Once you have a {class}`FilePattern <pangeo_forge_recipes.patterns.FilePattern>` object,
the recipe pipeline will contain at a minimum the following transforms applied to the file pattern collection:
* {class}`pangeo_forge_recipes.transforms.OpenURLWithFSSpec`: retrieves each pattern file using the specified URLs.
* {class}`pangeo_forge_recipes.transforms.OpenWithXarray`: load each pattern file into an [`xarray.Dataset`](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html).
* {class}`pangeo_forge_recipes.transforms.StoreToZarr`: generate a Zarr store by combining the datasets.
* {class}`pangeo_forge_recipes.transforms.ConsolidateDimensionCoordinates`: consolidate the Dimension Coordinates for dataset read performance.
* {class}`pangeo_forge_recipes.transforms.ConsolidateMetadata`: calls Zarr's convinience function to consolidate metadata.

### Open existing Zarr Store
* {class}`pangeo_forge_recipes.transforms.OpenWithXarray` supports opening existing Zarr stores. This might be useful for rechunking a Zarr store into an alternative chunking scheme.
An example of this recipe can be found in - {doc}`examples/gpcp-rechunk`






```{tip}
If using the {class}`pangeo_forge_recipes.transforms.ConsolidateDimensionCoordinates` transform, make sure to chain on the {class}`pangeo_forge_recipes.transforms.ConsolidateMetadata` transform to your recipe.

```

```{note}
{class}`pangeo_forge_recipes.transforms.StoreToZarr` supports appending to existing Zarr stores
via the optional `append_dim` keyword argument. This option functions nearly identically to the
`append_dim` kwarg in
[`xarray.Dataset.to_zarr`](https://docs.xarray.dev/en/latest/generated/xarray.Dataset.to_zarr.html);
the two differences with this method are that Pangeo Forge will automatically introspect the inputs in
your {class}`FilePattern <pangeo_forge_recipes.patterns.FilePattern>` to determine how the existing Zarr
store dimensions need to be resized, and that writes are parallelized via Apache Beam. Apart from
ensuring that the named `append_dim` already exists in the dataset to which you are appending, use of
this option does not ensure logical consistency (e.g. contiguousness, etc.) of the appended data. When
selecting this option, it is therefore up to you, the user, to ensure that the inputs provided in the
{doc}`file pattern <file_patterns>` for the appending recipe are limited to those which you want to
append.
```
