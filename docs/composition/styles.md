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

```{tip}
If using the {class}`pangeo_forge_recipes.transforms.ConsolidateDimensionCoordinates` transform, make sure to chain on the {class}`pangeo_forge_recipes.transforms.ConsolidateMetadata` transform to your recipe.

```


## Open with Kerchunk, write to virtual Zarr

The standard Zarr recipe creates a copy of the original dataset in the Zarr format, this
[kerchunk](https://fsspec.github.io/kerchunk/)-based reference recipe style does not copy the
data and instead creates a Kerchunk mapping, which allows archival formats (including NetCDF, GRIB2, etc.) to be read _as if_ they were Zarr datasets. More details about how Kerchunk works can be found in the
[kerchunk docs](https://fsspec.github.io/kerchunk/detail.html) and
[this blog post](https://medium.com/pangeo/fake-it-until-you-make-it-reading-goes-netcdf4-data-on-aws-s3-as-zarr-for-rapid-data-access-61e33f8fe685).

```{note}
Examples of this recipe style currently exist in development form, and will be cited here as soon as they
are integration tested, which is pending <https://github.com/pangeo-forge/pangeo-forge-recipes/issues/608>.
```

### Is this style right for my dataset?

For archival data stored on highly-throughput storage devices, and for which
preprocessing is not required, reference recipes are an ideal and storage-efficient option.
When choosing whether to create a reference recipe, it is important to consider questions such as:

#### Where are the archival (i.e. source) files for this dataset currently stored?

If the original data are not already in the cloud (or some other high-bandwidth storage device,
such as an on-prem data center), the performance benefits of using a reference recipe may be limited,
because network speeds to access the original data will constrain I/O throughput.

#### Does this dataset require preprocessing?

With reference recipes, modification of the underlying data is not possible. For example, the
chunking schema of a dataset cannot be modified with Kerchunk, so you are limited to the chunk schema of the
archival data. If you need to optimize your datasets chunking schema for space or time, the standard Zarr
recipe is the only option. While you cannot modify chunking in a reference recipe, changes in the metadata
(attributes, encoding, etc.) can be applied.
