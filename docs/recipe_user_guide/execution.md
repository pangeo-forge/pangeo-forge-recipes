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

Each recipe class defines its own sequence of execution stages which
collectively represent the execution `pipeline` for the given class. The
pipeline for any recipe can be assigned to a local variable as follows:

```{code-block} python
pipeline = recipe.to_pipeline()
```

The names of the sequential execution stages for a given recipe class are
returned by calling Python's built-in `list()` function on your `pipeline`.
These stage names may vary between recipe classes. For the `XarrayZarrRecipe`,
they look like this:

```{code-block} python
>>> list(pipeline)

['cache_input', 'prepare_target', 'store_chunk', 'finalize_target']
```

Any of these names can be used to select the cooresponding stage object from the
`pipeline`, for example:

```{code-block} python
>>> pipeline["prepare_target"]

Stage(function=<function prepare_target at 0x178ad6670>, name='prepare_target', mappable=None, annotations=None)
```

### Mappable vs. non-mappable

When manually executing a `pipeline`, the most import things to be aware of are
execution sequence and whether or not the function for a given stage is mappable.
Mappable stage functions take positional arguments and are manually executed in a loop,
with a different positional argument passed to each successive function call in the loop.
Non-mappable functions, by contrast, take no positional arguments and are called only once.

The simplest way to determine if a stage is mappable or not is by checking the `ismappable` property:

```{code-block} python
>>> pipeline["cache_input"].ismappable
True

>>> pipeline["prepare_target"].ismappable
False
```

In what follows we will see how to apply these concepts to execute our `pipeline`'s four stages.

### Example

In the previous section, we saw how to determine the sequential stages of a pipeline with
`list(pipeline)` and how to determine if a stage is mappable with `pipeline["stage_name"].ismappable`.
Now that we know the names and order of our `pipeline` stages, as well as how to determine if
a given stage is mappable, we can move on to manually executing the `pipeline`.

```{note}
All stages, regardless of whether they are mappable or not, take a single keyword argument,
`config=recipe`, where `recipe` is the same recipe instance used to create the `pipeline`.
```

#### Stage 1: Cache Inputs

We've seen from the result of `list(pipeline)` that the first stage of our `pipeline` is `"cache_input"`.

> Recipes may define files that have to be cached locally before the subsequent
steps can proceed. The common use case here is for files that have to be
extracted from a slow FTP server. This is what the `"cache_input"` stage accomplishes.

We know that `pipeline["cache_input"].ismappable` returns `True`, which tells us we will
need to execute this stage in a loop. The positional arguments passed to each iteration of
the loop are found in the `mappable` attribute of the stage. Recalling that stage functions also take a `config=recipe` keyword argument, we can manually execute this stage as follows:

```{code-block} python
for m in pipeline["cache_input"].mappable:
    pipeline["cache_input"].function(m, config=recipe)
```


#### Stage 2: Prepare Target

Once again, we know from `list(pipeline)` that the second stage of our `pipeline` is `"prepare_target"`. This stage is non-mappable, therefore we call it just once, as follows:

```{code-block} python
pipeline["prepare_target"].function(config=recipe)
```

> In the `XarrayZarrRecipe`, this sets up the Zarr group with the necessary
arrays and metadata.

#### Stage 3: Store Chunks

This mappable step is where the bulk of the work happens for the `XarrayZarrRecipe`.

```{code-block} python
for m in pipeline["store_chunk"].mappable:
    pipeline["store_chunk"].function(m, config=recipe)
```

#### Stage 4: Finalize Target

Metadata cleanup and consolidation happens in this non-mappable step.

```{code-block} python
pipeline["finalize_target"].function(config=recipe)
```

### Summary

In the preceding example, we've seen how to manually execute the stages of a
`pipeline` for the `XarrayZarrRecipe`. The general principles conveyed in this example are
transferable to any recipe class. For any recipe class, a `pipeline` can be defined with:

```{code-block} python
pipeline = recipe.to_pipeline()
```

The `pipeline`'s series of sequential stage names can be retrieved via:

```{code-block} python
list(pipeline)
```

And those stages can be sequentially executed by name with, for mappable stages:

```{code-block} python
for m in pipeline["stage_name"].mappable:
    pipeline["stage_name"].function(m, config=recipe)
```

Or, for non-mappable stages:

```{code-block} python
pipeline["stage_name"].function(config=recipe)
```

## Compiled Recipes

Very large recipes cannot feasibly be executed manually.
Instead, recipes can be _compiled_ to executable objects.
We currently support four types of compilation.

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

You can convert your recipe to a [Prefect Flow](https://docs.prefect.io/core/concepts/flows.html) using
the :meth:`BaseRecipe.to_prefect()` method. For example

```{code-block} python
flow = recipe.to_prefect()
flow.run()
```

By default the flow is run using Prefect's [LocalExecutor](https://docs.prefect.io/orchestration/flow_config/executors.html#localexecutor). See [executors](https://docs.prefect.io/orchestration/flow_config/executors.html) for more.

### Beam PTransform

You can convert your recipe to an Apache Beam [PTransform](https://beam.apache.org/documentation/programming-guide/#transforms)
to be used within a [Pipeline](https://beam.apache.org/documentation/programming-guide/#creating-a-pipeline) using the
:meth:`BaseRecipe.to_beam()` method. For example

```{code-block} python
import apache_beam as beam

with beam.Pipeline() as p:
   p | recipe.to_beam()
```

By default the pipeline runs using Beam's [DirectRunner](https://beam.apache.org/documentation/runners/direct/).
See [runners](https://beam.apache.org/documentation/#runners) for more.
