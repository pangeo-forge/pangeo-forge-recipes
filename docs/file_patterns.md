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
this is what a {class}`pangeo_forge.patterns.FilePattern` is meant to describe.

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
- {class}`pangeo_forge.patterns.ConcatDim`: The files should be combined by
  concatenating _the same variables_ sequentially along an axis.
  This is conceptually similar to Xarray's [concat](http://xarray.pydata.org/en/stable/combining.html#concatenate)
  operation.
- {class}`pangeo_forge.patterns.MergeDim`: The files be combined by merging
  _multiple distinct variables_ into a single dataset. This is conceptually
  similar to Xarray's [merge](http://xarray.pydata.org/en/stable/combining.html#merge)
  operation.

File patterns permit us to combine multiple combine dims into a single pattern.
For the present example, we have one ``MergeDim``:

```{code-cell} ipython3
from pangeo_forge.patterns import MergeDim
variable_merge_dim = MergeDim("variable", ["temperature", "humidity"])
```

...and one ``ConcatDim``:

```{code-cell} ipython3
from pangeo_forge.patterns import ConcatDim
time_concat_dim = ConcatDim("time", list(range(1, 11)))
```

We are now ready to create our file pattern. We do this by bringing together
the function which generates the file names with the merge dimensions.

```{code-cell} ipython3
from pangeo_forge.patterns import FilePattern
pattern = FilePattern(make_full_path, variable_merge_dim, time_concat_dim)
pattern
```

```{note}
The names of the arguments in your function are important: they must match the keys of
your file pattern. The keys of the file pattern are determined by the names of the combine dims.
Note that the function ``make_full_path`` has two arguments: ``variable``, and ``time``.
These are precisely the same as the names of the combine dimension used in
defining ``variable_merge_dim`` and ``time_concat_dim``.
You can use whatever names you want,but they must be consistent throughout your file pattern.
```

In theory, we could use any number of combine dimensions.
However, in practice, recipe implementations may have limits on the number of
and type of combine dimensions they support.
{class}`pangeo_forge.recipes.XarrayZarrRecipe` requires at least one
``ConcatDim`` and allows at most one ``MergeDim``.

### Inspect a `FilePattern`

Normally at this point we would just move on and pass our FilePattern
object to a {doc}`recipe <recipes>` constructor.
But we can also inspect the FilePattern manually to understand how it works.

Internally, the FilePattern maps the keys of the various combine dims to logical indices.
We can see all of these keys by iterating over the patterns using the ``items()`` method:

```{code-cell} ipython3
for index, fname in pattern.items():
    print(index, fname)
```

And we can get any of the filenames back out by using "getitem" syntax on the FilePattern

```{code-cell} ipython3
pattern[1, 4]
```

It isn't necessary to do any of these things to create a recipe; however digging into
a FilePattern's internals may be helpful in debugging a complex recipe.

### Specifying `nitems_per_input` in a `ConcatDim`

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
provide this as a hint, via `niterms_per_file` keyword in `ConcatDim`.
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

## File Patterns API

```{eval-rst}
.. autoclass:: pangeo_forge.patterns.FilePattern
    :members:
    :special-members: __getitem__, __iter__
```


```{eval-rst}
.. autoclass:: pangeo_forge.patterns.ConcatDim
    :members:
```


```{eval-rst}
.. autoclass:: pangeo_forge.patterns.MergeDim
    :members:
```
