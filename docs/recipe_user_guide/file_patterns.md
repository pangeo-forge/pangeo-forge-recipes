---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# File Patterns

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

    ```ipython3
    fsspec_open_kwargs={"username": "<your-username>", "password": "<your-password>"}
    fsspec_open_kwargs={"auth": aiohttp.BasicAuth("<your-username>", "<your-password>")}
    ```

- **`query_string_secrets`**: A dictionary of key:value pairs to append to each source file url query at runtime. Query
parameters which are not secrets should instead be included in the `format_function`.
- **`is_opendap`**: Boolean value to specify whether or not the source files are served via OPeNDAP. Incompatible with caching,
and mutually exclusive with `fsspec_open_kwargs`. Defaults to `False`.

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

Normally at this point we would just move on and pass our FilePattern
object to a {doc}`recipe <recipes>` constructor.
But we can also inspect the FilePattern manually to understand how it works.
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

```{eval-rst}
.. autoclass:: pangeo_forge_recipes.patterns.FilePattern
    :members:
    :special-members: __getitem__, __iter__
```

### Combine Dimensions

```{eval-rst}
.. autoclass:: pangeo_forge_recipes.patterns.ConcatDim
    :members:
```


```{eval-rst}
.. autoclass:: pangeo_forge_recipes.patterns.MergeDim
    :members:
```

### Indexing

```{eval-rst}
.. autoclass:: pangeo_forge_recipes.patterns.Index
    :members:
```

```{eval-rst}
.. autoclass:: pangeo_forge_recipes.patterns.DimIndex
    :members:
```

```{eval-rst}
.. autoclass:: pangeo_forge_recipes.patterns.CombineOp
    :members:
```
