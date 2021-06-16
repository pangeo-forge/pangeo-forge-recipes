# Recipe Execution

There are many different types of Pangeo Forge recipes.
However, **all recipes are executed the same way**!
This is a key part of the Pangeo Forge design.

Once you have created a recipe object (see {doc}`recipes`) you have two
options for executing it. In the subsequent code, we will assume that a
recipe has already been initialized in the variable `recipe`.

## Manual Execution

A recipe can be executed manually, step by step, in serial, from a notebook
or interactive interpreter. The ability to manually step through a recipe
is very important for developing and debugging complex recipes.
There are four stages of recipe execution.

### Stage 1: Cache Inputs

Recipes may define files that have to be cached locally before the subsequent
steps can proceed. The common use case here is for files that have to be
extracted from a slow FTP server. Here is how to cache the inputs.

```{code-block} python
for input_name in recipe.iter_inputs():
    recipe.cache_input(input_name)
```

If the recipe doesn't do input caching, nothing will happen here.

### Stage 2: Prepare Target

Once the inputs have been cached, we can get the target ready.
Preparing the target for writing is done as follows:

```{code-block} python
recipe.prepare_target()
```

For example, for Zarr targets, this sets up the Zarr group with the necessary
arrays and metadata.
This is the most complex step, and the most likely place to get an error.

### Stage 3: Store Chunks

This is the step where the bulk of the work happens.

```{code-block} python
for chunk in recipe.iter_chunks():
    recipe.store_chunk(chunk)
```

### Stage 4: Finalize Target

If there is any cleanup or consolidation to be done, it happens here.

```{code-block} python
recipe.finalize_target()
```

For example, consolidating Zarr metadata happens in the finalize step.

## Compiled Recipes

Very large recipes cannot feasibly be executed this way.
Instead, recipes can be _compiled_ to executable objects.
We currently support three types of compilation.

### Python Function

To convert a recipe to a single python function, use the method `.to_function()`.
For example

```{code-block} python
recipe_func = recipe.to_function()
recipe_func()  # actually execute the recipe
```

Note that the python function approach does not support parallel or distributed execution.
It's mostly just a convenience utility.


### Dask Delayed

You can convert your recipe to a [Dask Delayed](https://docs.dask.org/en/latest/delayed.html)
object using the `.to_dask()` method. For example

```{code-block} python
delayed = recipe.to_dask()
delayed.compute()
```

The `delayed` object can be executed by any of Dask's schedulers, including
cloud and HPC distributed schedulers.

### Prefect Flow

TODO...
