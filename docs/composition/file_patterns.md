---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# File patterns

## Introduction

### File patterns define recipe inputs

`FilePattern`s are the starting point for any Pangeo Forge recipe. They are the raw
inputs (or "ingredients") upon which the recipe will act. File patterns describe:

- Where individual source files are located; and
- How they should be organized logically as part of an aggregate dataset.
(In this respect, file patterns are conceptually similar to
[NcML](https://docs.unidata.ucar.edu/netcdf-java/current/userguide/ncml_aggregation.html) documents.)

```{note}
API Reference is available here: {class}`pangeo_forge_recipes.patterns.FilePattern`
```

### Pangeo Forge Pulls Data

A central concept in Pangeo Forge is that data are "pulled", not "pushed" to
the storage location. A file pattern describes where to find your data; when you
execute the recipe, the data will automatically be fetched and transformed.
You cannot "upload" data to Pangeo Forge. This is deliberate.

For recipes built from public, open data, it's always best to try to get the data
from its original, authoritative source. For example, if you want to use satellite
data from NASA, you need to find the URLs which point to that data on NASA's servers.

Pangeo Forge supports a huge range of different transfer protocols for accessing
URL-based data files, thanks to the [filesystem-spec](https://filesystem-spec.readthedocs.io/)
framework. A full list of protocols can be found in the fsspec docs
([built-in implementations](https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations) |
[other implementations](https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations)).
In order for Pangeo Forge to pull your data, it should be accessible over the public internet
via one of these protocols.

```{tip}
To access data stored on HPC filesystems, {doc}`../advanced/globus` may be useful.
```

## Create a file pattern

Let's explore a simple example of how to create a file pattern for an
imaginary dataset with file paths which look like this:

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
- There are two different "variables" (temperature and humidity), stored in separate files.
- There is a sequence of 10 files for each variable. We will assume that this
  represents the "time" axis of the data.

We observe that there are essentially two **dimensions** to the file organization:
variable (2) and time (10). The product of these (2 x 10 = 20) determines the total
number of files in our dataset.
We refer to the unique identifiers for each dimension (`temperature`, `humidity`; `1, 2, ..., 10`)
as the **keys** for our file pattern.
At this point, we don't really care what is _inside_ these files.
We are just interested in the logical organization of the files themselves;
this is what a `FilePattern` is meant to describe.

### Sneak peek: the full code

Here is the full code we will be writing below to describe a file pattern for this
imaginary dataset, provided upfront for reference:

```{code-cell} ipython3
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim

def make_full_path(variable, time):
    return f"http://data-provider.org/data/{variable}/{variable}_{time:02d}.txt"

variable_merge_dim = MergeDim("variable", ["temperature", "humidity"])
time_concat_dim = ConcatDim("time", list(range(1, 11)))

kws = {}  # no keyword arguments used for this example
pattern = FilePattern(make_full_path, variable_merge_dim, time_concat_dim, **kws)
pattern
```

In what follows we will look at each element of this code one-by-one.

### Format function

The starting point for creating a file pattern is to write a function which maps
the keys for each dimension into full file paths. This function might look something
like this:

```{code-cell} ipython3
def make_full_path(variable, time):
    return f"http://data-provider.org/data/{variable}/{variable}_{time:02d}.txt"

# check that it works
make_full_path("humidity", 3)
```

```{important}
Argument names in your [](#format-function) must match the names
used in your [](#combine-dimensions).
Here, the function ``make_full_path`` has two arguments: ``variable``, and ``time``.
These are the same as the names used in our [](#combine-dimensions).
```

### Combine dimensions

We now need to define the "combine dimensions" of the file pattern.
Combine dimensions are one of two types:
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

### Keyword arguments

`FilePattern` objects carry all of the information needed to open source files, which may include
source-server specific arguments and/or authentication information. These options can be specified
via keyword arguments. Please refer to the API Reference for more on these optional parameters:
{class}`pangeo_forge_recipes.patterns.FilePattern`.

```{warning}
Secrets including login credentials and API tokens should never be committed to a public repository. As such,
we strongly suggest that you do **not** instantiate your `FilePattern` with these or any other secrets when
developing your recipe. If your source files require authentication via [](#keyword-arguments), it is advisable to provide these values as variables in the {doc}`../deployment/index` environment, and _**not**_
as literal values in the recipe file itself.
```

### Putting it all together

We are now ready to create our file pattern. We do this by bringing together
the [](#format-function), [](#combine-dimensions), and (optionally) any [](#keyword-arguments).

```{code-cell} ipython3
from pangeo_forge_recipes.patterns import FilePattern

kws = {}  # no keyword arguments used for this example
pattern = FilePattern(make_full_path, variable_merge_dim, time_concat_dim, **kws)
pattern
```

To see the full code in one place, please refer back to [](#sneak-peek-the-full-code).

## Inspect a `FilePattern`

We can inspect file patterns manually to understand how they work. This is not necessary
to create a recipe; however digging into a `FilePattern`'s internals may be helpful in
debugging a complex recipe. Internally, the file pattern maps the keys of the
[](#combine-dimensions) to logical indices. We can see all of these keys by iterating over
the patterns using the ``items()`` method:

```{code-cell} ipython3
for index, fname in pattern.items():
    print(index, fname)
```

```{hint}
This ``items()`` method will come up again in [](#from-file-pattern-to-pcollection).
```

The index is a {class}`pangeo_forge_recipes.patterns.Index` used internally by `pangeo-forge-recipes`
to align the source files in the aggregate dataset.
Users generally will not need to interact with indexes manually, but it may be interesting to
note that we can retrieve source filenames from the file pattern via "getitem" syntax, using
an index as key:

```{code-cell} ipython3
pattern[index]
```

## From file pattern to `PCollection`

As covered in {doc}`index`, a recipe is composed of a sequence of Apache Beam transforms.
The data collection that Apache Beam transforms operates on is a
[`PCollection`](https://beam.apache.org/documentation/programming-guide/#pcollections).
Therefore, we bring the contents of a `FilePattern` into a recipe, we pass the index:url
pairs generated by the file pattern's ``items()`` method into Beam's `Create` constructor
as follows:

```{code-cell} ipython3
import apache_beam as beam

recipe = (
  beam.Create(pattern.items())
  # ... continue with additional transforms here
)
```

We now have our data properly initialized, and can continue composing the recipe with
additional transforms from here, including {doc}`openers`, (optionally) {doc}`preprocess`,
and {doc}`writers`.
