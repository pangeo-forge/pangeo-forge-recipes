# Recipes

A recipe defines how to transform data in one format / location into another format / location.
The primary way people contribute to Pangeo Forge is by writing / maintaining recipes.

```{warning}
The Recipe API is still in flux and may change. Make sure the version of the documentation
you are reading matches your installed version of pangeo_forge.
```

## Storage

Recipes need a place to store data.
The location where the final dataset produced by the recipe is stored is called the
``Target``. Pangeo forge has a special class for this: {class}`pangeo_forge.storage.FSSpecTarget`

Creating a Target requires two arguments:
- The ``fs`` argument is an [fsspec](https://filesystem-spec.readthedocs.io/en/latest/)
  filesystem. Fsspec supports many different types of storage via its
  [built in](https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations)
  and [third party](https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations)
  implementations.
- The `root_path` argument specifies the specific path where the data should be stored.

For example, creating a storage target for AWS S3 might look like this:
```{code-block} python
import s3fs
fs = s3fs.S3FileSystem(key="MY_AWS_KEY", secret="MY_AWS_SECRET")
target_path = "pangeo-forge-bucket/my-dataset-v1.zarr"
target = FSSpecTarget(fs=fs, root_path=target_path)
```

Temporary data is recommended to use a {class}`pangeo_forge.storage.CacheFSSpecTarget` object.

## The Recipe Object

You define a recipe by instantiating a class that inherits from {class}`pangeo_forge.recipe.BaseRecipe`.
The `pangeo_forge` package includes several pre-defined Recipe classes which
cover common scenarios. You can also define your own Recipe class.

For a the common scenario of assembling many NetCDF files into a single Zarr
group, we use {class}`pangeo_forge.recipe.NetCDFtoZarrSequentialRecipe`.
Initializing a recipe looks something like this.

```{code-block} python
from pangeo_forge.recipes import NetCDFtoZarrSequentialRecipe
input_urls = [...]  # build a list of inputs
recipe = NetCDFtoZarrSequentialRecipe(
    input_urls=input_urls,
    sequence_dim="time"
)
```

There are many other options we can pass, all covered in the [API documentation](api).
For a deeper dive on how to pick these options and what they mean, check out the
tutorial: {doc}`tutorials/netcdf_zarr_sequential`.

Your recipe will also need storage.
If you have already defined a `Target` object (as in the the [Storage section](#storage)),
then you can either assign it when you initialize the recipe or later, e.g.

```{code-block} python
recipe.target = FSSpecTarget(fs=fs, root_path=target_path)
```

This particular class of recipe also requires a cache, a place to store temporary
files. We can create one as follows.

```{code-block} python
recipe.input_cache = CacheFSSpecTarget(fs=fs, root_path=cache_path)
```

Once your recipe is defined and has its targets assigned, you're ready to
move on to {doc}`execution`.
