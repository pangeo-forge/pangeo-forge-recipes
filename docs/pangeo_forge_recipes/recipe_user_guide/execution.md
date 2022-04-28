# Recipe Execution

There are many different types of Pangeo Forge recipes.
However, **all recipes are executed the same way**!
This is a key part of the Pangeo Forge design.

Once you have created a recipe object (see {doc}`recipes`) you have two
options for executing it. In the subsequent code, we will assume that a
recipe has already been initialized in the variable `recipe`.


## Recipes Executors

```{note}
API reference documentation for execution can be found in {mod}`pangeo_forge_recipes.executors`.
```

A recipe is an abstract description of a transformation pipeline.
Recipes can be _compiled_ to executable objects.
We currently support three types of compilation.

### Python Function

To compile a recipe to a single python function, use the method `.to_function()`.
For example

```{code-block} python
recipe_func = recipe.to_function()
recipe_func()  # actually execute the recipe
```

Note that the python function approach does not support parallel or distributed execution.
It's mostly just a convenience utility.


### Dask Delayed

You can compile your recipe to a [Dask Delayed](https://docs.dask.org/en/latest/delayed.html)
object using the `.to_dask()` method. For example

```{code-block} python
delayed = recipe.to_dask()
delayed.compute()
```

The `delayed` object can be executed by any of Dask's schedulers, including
cloud and HPC distributed schedulers.

### Prefect Flow

You can compile your recipe to a [Prefect Flow](https://docs.prefect.io/core/concepts/flows.html) using
the :meth:`BaseRecipe.to_prefect()` method.

There are two modes of Prefect execution.
In the default, every individual step in the recipe is explicitly represented
as a distinct Prefect Task within a Flow.

:::{warning}
For large recipes, this default can lead to Prefect Flows with >10000 Tasks.
In our experience, Prefect can struggle with this volume.
:::

```{code-block} python
flow = recipe.to_prefect()
flow.run()
```

By default the flow is run using Prefect's [LocalExecutor](https://docs.prefect.io/orchestration/flow_config/executors.html#localexecutor). See [executors](https://docs.prefect.io/orchestration/flow_config/executors.html) for more.

An alternative is to create _a single Prefect Task_ for the entire Recipe.
This task wraps a Dask Delayed graph, which can then be scheduled on
a Dask cluster. To use this mode, pass the option `wrap_dask=True`:

```{code-block} python
flow = recipe.to_prefect(wrap_dask=True)
flow.run()
```

### Beam PTransform

You can compile your recipe to an Apache Beam [PTransform](https://beam.apache.org/documentation/programming-guide/#transforms)
to be used within a [Pipeline](https://beam.apache.org/documentation/programming-guide/#creating-a-pipeline) using the
:meth:`BaseRecipe.to_beam()` method. For example

```{code-block} python
import apache_beam as beam

with beam.Pipeline() as p:
   p | recipe.to_beam()
```

By default the pipeline runs using Beam's [DirectRunner](https://beam.apache.org/documentation/runners/direct/).
See [runners](https://beam.apache.org/documentation/#runners) for more.

## Manual Execution

```{warning}
We are considering dropping manual execution from the Pangeo Forge API.
```

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
