# Writers

Recipes need a place to store data. This information is provided to the recipe using the transforms in the corresponding pipeline, where storage configuration may include a *cache* location to store retrieved source data products, and a *target* location to store the recipe output.

```{eval-rst}
.. autoclass:: pangeo_forge_recipes.transforms.OpenURLWithFSSpec
    :noindex:
```
```{eval-rst}
.. autoclass:: pangeo_forge_recipes.transforms.StoreToZarr
    :noindex:
```

## Default storage

When you create a new recipe, it is common to specify storage locations pointing at a local [`tempfile.TemporaryDirectory`](https://docs.python.org/3/library/tempfile.html#tempfile.TemporaryDirectory).
This allows you to write data to temporary local storage during the recipe development and debugging process.
This means that any recipe can immediately be executed with minimal configuration.
However, in a realistic "production" scenario, a separate location will be used. In all cases, the storage locations are customized using the corresponding transform parameters.

## Customizing *target* storage: `StoreToZarr`

The minimal requirement for instantiating `StoreToZarr` is a location in which to store the final dataset produced by the recipe. This is acheieved with the following parameters:

* `store_name` specifies the name of the generated Zarr store.
* `target_root` specifies where the output will be stored. For example, a temporary directory created during local development.

Although `target_root` may be a `str` pointing to a location, it also accepts a special class provided by Pangeo Forge for this: {class}`pangeo_forge_recipes.storage.FSSpecTarget`. Creating an ``FSSpecTarget`` requires two arguments:
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
target_root = FSSpecTarget(fs=fs, root_path="pangeo-forge-bucket")
```

This target can then be assiged to a recipe as follows (see also {doc}`examples/index`):
```{code-block} python
transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern.file_type)
    | StoreToZarr(
        store_name="my-dataset-v1.zarr",
        target_root=target_root,
        combine_dims=pattern.combine_dim_keys,
        target_chunks={"time": 10}
    )
)
```

## Customizing storage continued: caching with `OpenURLWithFSSpec`

Oftentimes it is useful to cache input files, rather than read them directly from the data provider. Input files can be cached at a location defined by a {class}`pangeo_forge_recipes.storage.CacheFSSpecTarget` object. For example, extending the previous recipe pipeline:

```{code-block} python

from pangeo_forge_recipes.storage import CacheFSSpecTarget, FSSpecTarget, MetadataTarget, StorageConfig

# define your fsspec filesystems for the target, cache, and metadata locations here

cache = CacheFSSpecTarget(fs=<fsspec-filesystem-for-cache>, root_path="<path-for-cache>")

transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(cache=cache)
    | OpenWithXarray(file_type=pattern.file_type)
    | StoreToZarr(
        store_name="my-dataset-v1.zarr",
        target_root=target_root,
        combine_dims=pattern.combine_dim_keys,
        target_chunks={"time": 10}
    )
```
