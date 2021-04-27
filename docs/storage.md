# Storage

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

Temporary data is can be cached via a {class}`pangeo_forge.storage.CacheFSSpecTarget` object.
Some recipes require separate caching of metadata, which is provided by a third {class}`pangeo_forge.storage.FSSpecTarget`.

```{eval-rst}
.. autoclass:: pangeo_forge.storage.FSSpecTarget
    :members:
```

```{eval-rst}
.. autoclass:: pangeo_forge.storage.FlatFSSpecTarget
    :members:
    :show-inheritance:
```

```{eval-rst}
.. autoclass:: pangeo_forge.storage.CacheFSSpecTarget
    :members:
    :show-inheritance:
```
