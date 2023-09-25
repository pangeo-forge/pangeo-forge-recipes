---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# File patterns

`FilePatterns` are the starting point for any Pangeo Forge recipe. They are the raw "ingredients"
upon which the recipe will act. The point of file patterns is to describe how many individual source
files are organized logically as part of a larger dataset.

Specifically, a recipe is a set of transforms which operate on an `FilePattern` materialized in the form
of an [`apache_beam.PCollection`](https://beam.apache.org/documentation/programming-guide/#pcollections).

## Pangeo Forge Pulls Data

A central concept in Pangeo Forge is that data are "pulled", not "pushed" to
the storage location. You tell Pangeo Forge where to find your data; when you
execute a recipe, the data will automatically be downloaded and transformed.
You cannot "upload" data to Pangeo Forge. This is deliberate.

There are basically two ways to tell Pangeo Forge where to find your data:
- Specify **file paths on your computer**: e.g. `/data/temperature/temperature_01.nc`;
  This works find if you are just running Pangeo Forge locally; however, it won't
  work when deploying to cloud resources, because those files are not accessible
  from the cloud. _File paths are different on every computer._
- Specify a **location on the internet via a [URL](https://developer.mozilla.org/en-US/docs/Learn/Common_questions/What_is_a_URL)**,
  e.g.`http://data-provider.org/data/temperature/temperature_01.nc`.
  URLs are more general than file paths because they are _the same on every computer_.
  Using URLs means that your recipe can be run anywhere on the internet.
  This is a requirement for Pangeo Forge Cloud.

For recipes built from public, open data, it's always best to try to get the data
from its original, authoritative source. For example, if you want to use satellite
data from NASA, you need to find the URLs which point to that data on NASA's servers.

## Supported Transfer Protocols

Pangeo Forge supports a huge range of different transfer protocols for accessing
URL-based data files, thanks to the [filesystem-spec](https://filesystem-spec.readthedocs.io/)
framework. A full list of protocols can be found in the fsspec docs
([built-in implementations](https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations) |
[other implementations](https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations)).

Some of the most important protocols commonly used with Pangeo Forge recipes are
- [Local Filesystem](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.local.LocalFileSystem)
- [HTTP](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.http.HTTPFileSystem)
- [FTP](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.ftp.FTPFileSystem)
- [SSH / SFTP](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.sftp.SFTPFileSystem)
- [S3](https://s3fs.readthedocs.io/en/latest/)
- [Google Cloud Storage](https://gcsfs.readthedocs.io/en/latest/)
- [Azure Datalake / Azure BLOB Storage](https://github.com/fsspec/adlfs)

In order for Pangeo Forge to pull your data, it should be accessible over the public internet
via one of those protocols.

## Globus

Many scientific datasets stored on secure HPC systems and servers are accessible
via the [Globus](https://www.globus.org/) file transfer service.
Globus uses a proprietary file transfer protocol, making it somewhat challenging
to integrate with an open-data system like Pangeo Forge.
In general, Globus file transfers require interaction with the Globus APIs,
which requires accounts and authentication.
Full support for Globus is on our roadmap. In the meantime, we recommend the
following workaround.

To ingest files from Globus with Pangeo Forge, you must create a
_fully public Guest Collection and access the files via HTTPS_.
The specific steps are as follows:
- Verify that the Globus endpoint you want to transfer from uses
  **Globus Connect Server V5**. This method _will not work_ with earlier Globus
  versions (e.g. V4).
- Open the [Globus App](https://app) and use the File Manager to navigate to
  the directory you want to share.
- Create a Public Guest Collection. Follow the Globus HOWTO instructions for
  - [Creating a Guest Collection](https://docs.globus.org/how-to/share-files/)
  - [Share data from a Guest Collection](https://docs.globus.org/how-to/guest-collection-share-and-access/).
    Under "Share With", you need to choose _public (anonymous) - make data accessible to everyone._
- Your data are now accessible via HTTPS. However, you need to figure out the URL.
  The easiest way to do this is via the Globus App. Right click on a specific file
  (not a directory!) within your Guest Collection and click "Get Link", then
  choose the option for "download the file directly". It should show an HTTPS link.
  Based on this link, you should be able to infer the relevant pattern for all
  your URLs.
  (For more details on HTTPS URLs, review the Globus docs on
  [HTTPS Access to Collections](https://docs.globus.org/globus-connect-server/v5/https-access-collections/).)

From here on, you should be able to point to your Globus files using standard
HTTPS URLs. You can proceed to create a File Pattern for your recipe.

## What are File Patterns?

File patterns are the starting point for any Pangeo Forge recipe:
they are the raw "ingredients" upon which the recipe will act.
The point of file patterns is to describe how many individual source files are
organized logically as part of a larger dataset.
In this respect, file patterns are conceptually similar to
[NCML](https://www.unidata.ucar.edu/software/netcdf-java/v4.5/ncml/index.htm) documents.

First we will describe a simple example of how to create a file pattern.
Then we will dive deeper into the API.

## Example

Imagine we have a set of file paths which look like this

```
http://data-provider.org/data/temperature/temperature_01.txt
http://data-provider.org/data/temperature/temperature_02.txt
...
http://data-provider.org/data/temperature/temperature_10.txt
http://data-provider.org/data/humidity/humidity_01.txt
http://data-provider.org/data/humidity/humidity_02.txt
...
http://data-provider.org/data/humidity/humidity_10.txt
```

This is a relatively common way to organize data files:
- There are two different "variables" (temperature and salinity), stored in separate files.
- There is a sequence of 10 files for each variable. We will assume that this
  represents the "time" axis of the data.

We observe that there are essentially two **dimensions** to the file organization:
variable (2) and time (10). The product of these (2 x 10 = 20) determines the total
number of files in our dataset.
We refer to the unique identifiers for each dimension (`temperature`, `humidity`; `1, 2, ..., 10`)
as the **keys** for our file pattern.
At this point, we don't really care what is _inside_ these files
We are just interested in the logical organization of the files themselves;
this is what a {class}`pangeo_forge_recipes.patterns.FilePattern` is meant to describe.

### Create a `FilePattern`

The starting point for creating a file pattern is to write a function which maps
the keys for each dimension into full file paths. This function might look something
like this:

```{code-cell} ipython3
def make_full_path(variable, time):
    return f"http://data-provider.org/data/{variable}/{variable}_{time:02d}.txt"

# check that it works
make_full_path("humidity", 3)
```

We now need to define the "combine dimensions" of the file pattern.
Comine dimensions are one of two types:
- {class}`pangeo_forge_recipes.patterns.ConcatDim`: The files should be combined by
  concatenating _the same variables_ sequentially along an axis.
  This is conceptually similar to Xarray's [concat](http://xarray.pydata.org/en/stable/combining.html#concatenate)
  operation.
- {class}`pangeo_forge_recipes.patterns.MergeDim`: The files be combined by merging
  _multiple distinct variables_ into a single dataset. This is conceptually
  similar to Xarray's [merge](http://xarray.pydata.org/en/stable/combining.html#merge)
  operation.

File patterns permit us to combine multiple combine dims into a single pattern.
For the present example, we have one ``MergeDim``:

```{code-cell} ipython3
from pangeo_forge_recipes.patterns import MergeDim
variable_merge_dim = MergeDim("variable", ["temperature", "humidity"])
```

...and one ``ConcatDim``:

```{code-cell} ipython3
from pangeo_forge_recipes.patterns import ConcatDim
time_concat_dim = ConcatDim("time", list(range(1, 11)))
```

We are now ready to create our file pattern. We do this by bringing together
the function which generates the file names with the merge dimensions.

```{code-cell} ipython3
from pangeo_forge_recipes.patterns import FilePattern
pattern = FilePattern(make_full_path, variable_merge_dim, time_concat_dim)
pattern
```

```{note}
The names of the arguments in your function are important: they must match the keys of
your file pattern. The keys of the file pattern are determined by the names of the combine dims.
Note that the function ``make_full_path`` has two arguments: ``variable``, and ``time``.
These are precisely the same as the names of the combine dimension used in
defining ``variable_merge_dim`` and ``time_concat_dim``.
You can use whatever names you want, but they must be consistent throughout your file pattern.
```

In theory, we could use any number of combine dimensions.
However, in practice, recipe implementations may have limits on the number of
and type of combine dimensions they support.
{class}`pangeo_forge_recipes.recipes.XarrayZarrRecipe` requires at least one
``ConcatDim`` and allows at most one ``MergeDim``.


### Extra keyword arguments for `FilePattern`

`FilePattern` objects carry all of the information needed to open source files. The following additional keyword
arguments may passed to `FilePattern` instances as appropriate:

- **`fsspec_open_kwargs`**: A dictionary of kwargs to pass to `fsspec.open` to aid opening of source files. For example,
`{"block_size": 0}` may be passed if an HTTP source file server does not permit range requests. Authentication for
`fsspec`-compatible filesystems may be handled here as well. For HTTP username/password-based authentication, your specific
`fsspec_open_kwargs` will depend on the configuration of the source file server, but are likely to conform to one of the following
two formats:

    ```python
    fsspec_open_kwargs={"username": "<your-username>", "password": "<your-password>"}
    fsspec_open_kwargs={"auth": aiohttp.BasicAuth("<your-username>", "<your-password>")}
    ```

- **`query_string_secrets`**: A dictionary of key:value pairs to append to each source file url query at runtime. Query
parameters which are not secrets should instead be included in the `format_function`.
- **`file_type`**: String value to specify the format of the input files. Can be one of `"netcdf3"`, `"netcdf4"`,
`"grib"`, `"opendap"`. `file_type="opendap"` is incompatible with caching, and mutually exclusive with
`fsspec_open_kwargs`. Defaults to `"netcdf4"`.

```{warning}
Secrets including login credentials and API tokens should never be committed to a public repository. As such,
we strongly suggest that you do **not** instantiate your `FilePattern` with these or any other secrets when
developing your recipe. If your source files require authentication via `fsspec_open_kwargs` and/or
`query_string_secrets`, it is advisable to update these attributes at execution time. Pangeo Forge will soon offer a
mechanism for securely handling such recipe secrets on GitHub.
```

### Specifying `nitems_per_file` in a `ConcatDim`

FilePatterns are deliberately very simple. However, there is one case where
we can annotate the FilePattern with a bit of extra information.
Combining files over a `ConcatDim` usually involves concatenating many records
belonging to a single physical or logical dimension in a sequence; for example,
if the `ConcatDim` is time, and we have one record per day, the recipe will
arrange every record in sequence in the target dataset.
An important piece of information is *how many records along the concat dim are in each file?*
For example, does the file `http://data-provider.org/data/temperature/temperature_01.txt`
have one record of daily temperature? Ten?
In general, Pangeo Forge does not assume there is a constant, known number of
records in each file; instead it will discover this information by peeking into each file.
But _if we know a-priori that there is a fixed number of records per file_, we can
provide this as a hint, via `nitems_per_file` keyword in `ConcatDim`.
Providing this hint will allow Pangeo Forge to work more quickly because it
doesn't have to peek into the files.

To be concrete, let's redefine our `ConcatDim` from the example above, now
assuming that there are 5 records per file in the `time` dimension.

```{code-cell} ipython3
time_concat_dim_fixed = ConcatDim("time", list(range(1, 11)), nitems_per_file=5)
pattern = FilePattern(make_full_path, variable_merge_dim, time_concat_dim_fixed)
pattern.concat_sequence_lens
```

We can see the the property `concat_sequence_lens` now exposes the total logical
length of the `time` dimension, which is very useful to recipes.

```{note}
The `nitems_per_file` keyword **only applies to size along the concat dim** (here "time").
In general files may have an arbitrary number of other dimensions that are not
concatenated, and we don't need to provide any hints about these.
```


### Inspect a `FilePattern`

We can inspect the FilePattern manually to understand how it works.
It isn't necessary to do any of these things to create a recipe; however digging into
a FilePattern's internals may be helpful in debugging a complex recipe.

Internally, the FilePattern maps the keys of the various combine dims to logical indices.
We can see all of these keys by iterating over the patterns using the ``items()`` method:

```{code-cell} ipython3
for index, fname in pattern.items():
    print(index, fname)
```

The index is its own special type of object used internally by recipes, a {class}`pangeo_forge_recipes.patterns.Index`,
(which is basically a tuple of one or more {class}`pangeo_forge_recipes.patterns.DimIndex` objects).
The index has a compact string representation, used for logging:
```{code-cell} ipython3
str(index)
```
and a more verbose `repr`:
```{code-cell} ipython3
repr(index)
```

Users generally will not need to create indexes manually,
but we can get any of the filenames back out by using "getitem" syntax on the FilePattern,
together with the index

```{code-cell} ipython3
pattern[index]
```


## File Patterns API
