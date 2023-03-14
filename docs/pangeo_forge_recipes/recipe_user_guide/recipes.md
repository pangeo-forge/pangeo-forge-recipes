# Recipes

A recipe defines how to transform data in one format / location into another format / location.
The primary way people contribute to Pangeo Forge is by writing / maintaining recipes.

```{note}
The Recipe API is under active development and may change. Make sure the version of the documentation you are reading matches your installed version of `pangeo-forge-recipes`. All released versions of `pangeo-forge-recipes` are supported in {doc}`../../pangeo_forge_cloud/index`. If you are starting a new recipe development cycle, it is recommend that you use the latest released version.
```

## The Recipe Object

A Recipe is a Python object which encapsulates a workflow for transforming data.
A Recipe knows how to take a {doc}`file pattern <file_patterns>`, which describes a collection of source files ("inputs"),
and turn it into a single analysis-ready, cloud-optimized dataset.
Creating a recipe does not actually cause any data to be read or written; the
recipe is just the _description_ of the transformation.
To actually do the work, the recipe must be {doc}`executed <execution>`.
Recipe authors (i.e. data users or data managers) can either execute their recipes
on their own computers and infrastructure, in private, or make a {doc}`../../pangeo_forge_cloud/recipe_contribution`
to {doc}`../../pangeo_forge_cloud/index`, which allows the recipe to be automatically by via [Bakeries](../../pangeo_forge_cloud/core_concepts.md).

## Recipe Pipelines

A recipe is defined as a [pipeline](https://beam.apache.org/documentation/programming-guide/#creating-a-pipeline) of [Apache Beam transforms](https://beam.apache.org/documentation/programming-guide/#transforms) applied to the data collection associated with a {doc}`file pattern <file_patterns>`. Specifically, each recipe pipeline contains a set of transforms, which operate on an [`apache_beam.PCollection`](https://beam.apache.org/documentation/programming-guide/#pcollections), performing a mapping of input elements to output elements (for example, using [`apache_beam.Map`](https://beam.apache.org/documentation/transforms/python/elementwise/map/)), applying the specified transformation.

To write a recipe, you define a pipeline that uses existing transforms, in combination with new transforms if required for custom processing of the input data collection.

Right now, there are two categories of recipe pipelines based on a specific data model for the input files and target dataset format.
In the future, we may add more.

```{note}
The full API Reference documentation for the existing recipe `PTransform` implementations ({class}`pangeo_forge_recipes.transforms`) can be found at
{doc}`../api_reference`.
```

### Xarray to Zarr Recipes


This recipe category uses
[Xarray](http://xarray.pydata.org/) to read the input files and
[Zarr](https://zarr.readthedocs.io/) as the target dataset format.
The inputs can be in any [file format Xarray can read](http://xarray.pydata.org/en/latest/user-guide/io.html),
including NetCDF, OPeNDAP, GRIB, Zarr, and, via [rasterio](https://rasterio.readthedocs.io/),
GeoTIFF and other geospatial raster formats.
The target Zarr dataset can be written to any storage location supported
by [filesystem-spec](https://filesystem-spec.readthedocs.io/); see {doc}`storage`
for more details.
The target Zarr dataset will conform to the
[Xarray Zarr encoding conventions](http://xarray.pydata.org/en/latest/internals/zarr-encoding-spec.html).

The best way to really understand how recipes work is to go through the relevant
tutorials for this recipe category. These are, in order of increasing complexity

- {doc}`../tutorials/xarray_zarr/netcdf_zarr_sequential`
- {doc}`../tutorials/xarray_zarr/cmip6-recipe`
- {doc}`../tutorials/xarray_zarr/multi_variable_recipe`
- {doc}`../tutorials/xarray_zarr/terraclimate`
- {doc}`../tutorials/xarray_zarr/opendap_subset_recipe`

Below we give a very basic overview of how this recipe is used.

First you must define a {doc}`file pattern <file_patterns>`.
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

The available transform options are all covered in the {doc}`../api_reference`. Many of these options are explored further in the {doc}`../tutorials/index`.

All recipes need a place to store the target dataset. Refer to {doc}`storage` for how to assign this and any other required storage targets.

Once your recipe is defined and has its storage targets assigned, you're ready to
move on to {doc}`execution`.

### HDF Reference Recipes

Like the Xarray to Zarr recipes, this category allows us to more efficiently access data from a bunch of NetCDF / HDF files.
However, such a recipe does not actually copy the original source data.
Instead, it generates metadata files which reference and index the original data, allowing it to be accessed more quickly and easily.
For more background, see [this blog post](https://medium.com/pangeo/fake-it-until-you-make-it-reading-goes-netcdf4-data-on-aws-s3-as-zarr-for-rapid-data-access-61e33f8fe685).

There is currently one tutorial for this recipe:

- {doc}`../tutorials/hdf_reference/reference_cmip6`
