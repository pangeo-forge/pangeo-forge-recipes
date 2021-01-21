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

## The Base Recipe Class

A recipe is initialized from a recipe class.
```{code-block} python
recipe = Recipe(option1='foo', option2=)
```

All recipes follow the same basic steps.


## Specific Recipe Classes

```{eval-rst}
.. autoclass:: pangeo_forge.recipe.NetCDFtoZarrSequentialRecipe
    :show-inheritance:
    :noindex:
```
