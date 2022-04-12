# Storage

Recipes need a place to store data. This information is provided to the recipe by its `.storage_config` attribute, which is an object of type {class}`pangeo_forge_recipes.storage.StorageConfig`.
The `StorageConfig` object looks like this

```{eval-rst}
.. autoclass:: pangeo_forge_recipes.storage.StorageConfig
    :noindex:
```

As shown above, the storage configuration includes three distinct parts: `target`, `cache`, and `metadata`.

## Default storage

When you create a new recipe, a default `StorageConfig` will automatically be created pointing at a local a local [`tempfile.TemporaryDirectory`](https://docs.python.org/3/library/tempfile.html#tempfile.TemporaryDirectory).
This allows you to write data to temporary local storage during the recipe development and debugging process.
This means that any recipe can immediately be executed with minimal configuration.
However, in a realistic "production" scenario, you will want to customize your storage locations.

## Customizing storage: the `target`

To write a recipe's full dataset to a persistant storage location, re-assign `.storage_config` to be a {class}`pangeo_forge_recipes.storage.StorageConfig` pointing to the location(s) of your choice. The minimal requirement for instantiating `StorageConfig` is a location in which to store the final dataset produced by the recipe. This is called the ``target``. Pangeo Forge has a special class for this: {class}`pangeo_forge_recipes.storage.FSSpecTarget`.

Creating a ``target`` requires two arguments:
- The ``fs`` argument is an [fsspec](https://filesystem-spec.readthedocs.io/en/latest/)
  filesystem. Fsspec supports many different types of storage via its
  [built in](https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations)
  and [third party](https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations)
  implementations.
- The `root_path` argument specifies the specific path where the data should be stored.

For example, creating a storage target for AWS S3 might look like this:
```{code-block} python
import s3fs
from pangeo_forge_recipes.storage import FSSpecTarget

fs = s3fs.S3FileSystem(key="MY_AWS_KEY", secret="MY_AWS_SECRET")
target_path = "pangeo-forge-bucket/my-dataset-v1.zarr"
target = FSSpecTarget(fs=fs, root_path=target_path)
```

This target can then be assiged to a recipe as follows:
```{code-block} python
from pangeo_forge_recipes.storage import StorageConfig

recipe.storage_config = StorageConfig(target)
```

Once assigned, the `target` can be accessed from the recipe with:

```{code-block} python
recipe.target
```

## Customizing storage continued: caching

Oftentimes it is useful to cache input files, rather than read them directly from the data provider. Input files can be cached at a location defined by a {class}`pangeo_forge_recipes.storage.CacheFSSpecTarget` object. Some recipes require separate caching of metadata, which is provided by a third class {class}`pangeo_forge_recipes.storage.MetadataTarget`.

A `StorageConfig` which declares all three storage locations is assigned as follows:

```{code-block} python

from pangeo_forge_recipes.storage import CacheFSSpecTarget, FSSpecTarget, MetadataTarget, StorageConfig

# define your fsspec filesystems for the target, cache, and metadata locations here

target = FSSpecTarget(fs=<fsspec-filesystem-for-target>, root_path="<path-for-target>")
cache = CacheFSSpecTarget(fs=<fsspec-filesystem-for-cache>, root_path="<path-for-cache>")
metadata = MetadataTarget(fs=<fsspec-filesystem-for-metadata>, root_path="<path-for-metadata>")

recipe.storage_config = StorageConfig(target, cache, metadata)
```
