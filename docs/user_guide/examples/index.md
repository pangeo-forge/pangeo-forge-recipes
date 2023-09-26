# Examples

## Xarray to Zarr Recipes

This recipe category uses [Xarray](http://xarray.pydata.org/) to open input files and
[Zarr](https://zarr.readthedocs.io/) as the target dataset format. Inputs can be in any
[file format Xarray can read](http://xarray.pydata.org/en/latest/user-guide/io.html),
including NetCDF, OPeNDAP, GRIB, Zarr, and, via [rasterio](https://rasterio.readthedocs.io/),
GeoTIFF and other geospatial raster formats.

The target Zarr dataset can be written to any storage location supported
by [filesystem-spec](https://filesystem-spec.readthedocs.io/); see {doc}`../writers`
for more details. The target Zarr dataset will conform to the
[Xarray Zarr encoding conventions](http://xarray.pydata.org/en/latest/internals/zarr-encoding-spec.html).

The best way to really understand how recipes work is to go through the relevant
examples for this recipe category:

- {doc}`gpcp-from-gcs`
- {doc}`noaa-oisst`


Below we give a very basic overview of how this recipe is used.

First you must define a {doc}`file pattern <../file_patterns>`.
Once you have a {class}`FilePattern <pangeo_forge_recipes.patterns.FilePattern>` object,
the recipe pipeline will contain at a minimum the following transforms applied to the file pattern collection:
* {class}`pangeo_forge_recipes.transforms.OpenURLWithFSSpec`: retrieves each pattern file using the specified URLs.
* {class}`pangeo_forge_recipes.transforms.OpenWithXarray`: load each pattern file into an [`xarray.Dataset`](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html):
  * The `file_type` is specified from the pattern.
* {class}`pangeo_forge_recipes.transforms.StoreToZarr`: generate a Zarr store by combining the datasets:
  * `store_name` specifies the name of the generated Zarr store.
  * `target_root` specifies where the output will be stored, in this case, the temporary directory we created.
  * `combine_dims` informs the transform of the dimension used to combine the datasets. Here we use the dimension specified in the file pattern (`time`).
  * `target_chunks`: specifies a dictionary of required chunk size per dimension. In the event that this is not specified for a particular dimension, it will default to the corresponding full shape.

For example:
```{code-block} python
transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern.file_type)
    | StoreToZarr(
        store_name=store_name,
        target_root=target_root,
        combine_dims=pattern.combine_dim_keys,
        target_chunks={"time": 10}
    )
)
```

The available transform options are all covered in the {doc}`../../api_reference`. Many of these options are explored further in the {doc}`../index`.

All recipes need a place to store the target dataset. Refer to {doc}`../writers` for how to assign this and any other required storage targets.

Once your recipe is defined and has its storage targets assigned, you're ready to
move on to {doc}`../../deployment/index`.

## Reference Recipes

Like the Xarray to Zarr recipes, this category of recipes allows us to efficiently access data from a
collection of source files. Unlike the standard Zarr recipes, these reference recipes utilize
[kerchunk](https://fsspec.github.io/kerchunk/) to generate metadata files which reference and index the
original data, allowing it to be accessed more quickly and easily, without duplicating it.

Whereas the standard Zarr recipe creates a copy of the original dataset in the Zarr format, the
kerchunk-based reference recipe does not copy the data and instead creates a Kerchunk mapping, which
allows archival formats (including NetCDF, GRIB2, etc.) to be read as if they were Zarr datasets.
More details about how Kerchunk works can be found in the
[kerchunk docs](https://fsspec.github.io/kerchunk/detail.html)
and [this blog post](https://medium.com/pangeo/fake-it-until-you-make-it-reading-goes-netcdf4-data-on-aws-s3-as-zarr-for-rapid-data-access-61e33f8fe685).

There are currently two tutorials for reference recipes:

- TODO: these recipes exist, but are not yet integration tested, so are ommitted here for now.

When choosing whether to create a reference recipe, it is important to consider questions such as:

_**Where are the archival (i.e. source) files for this dataset currently stored?**_ If the original data
are not already in the cloud (or some other high-bandwidth storage device, such as an on-prem data
center), the performance benefits of using a reference recipe may be limited, because network speeds
to access the original data will constrain I/O throughput.

_**Does this dataset require preprocessing?**_ With reference recipes, modification of the underlying
data is not possible. For example, the chunking schema of a dataset cannot be modified with Kerchunk, so
you are limited to the chunk schema of the archival data. If you need to optimize your datasets chunking
schema for space or time, the standard Zarr recipe is the only option. While you cannot modify chunking
in a reference recipe, changes in the metadata (attributes, encoding, etc.) can be applied.

These caveats aside, for archival data stored on highly-throughput storage devices, for which
preprocessing is not required, reference recipes are an ideal and storage-efficient option.


```{toctree}
:maxdepth: 1

noaa-oisst
gpcp-from-gcs
```
