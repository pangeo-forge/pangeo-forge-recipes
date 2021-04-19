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

For example, consolidating Zarr metadta happens in the finalize step.

## Execution by Executors

Very large recipes cannot feasibly be executed this way.
To support distributed parallel execution, Pangeo Forge borrows the
[Executors framework from Rechunker](https://rechunker.readthedocs.io/en/latest/executors.html).

There are currently three executors implemented.
- {class}`pangeo_forge.executors.PythonPipelineExecutor`: a reference executor
  using simple python
- {class}`pangeo_forge.executors.DaskPipelineExecutor`: distributed executor using Dask
- {class}`pangeo_forge.executors.PrefectPipelineExecutor`: distributed executor using Prefect

To use an executor, the recipe must first be transformed into a `Pipeline` object.
The full process looks like this:

```{code-block} python
pipeline = recipe.to_pipelines()
executor = PrefectPipelineExecutor()
plan = executor.pipelines_to_plan(pipeline)
executor.execute_plan(plan)  # actually runs the recipe
```

## Executors

```{eval-rst}
.. autoclass:: pangeo_forge.executors.PythonPipelineExecutor
    :members:
```

```{eval-rst}
.. autoclass:: pangeo_forge.executors.DaskPipelineExecutor
    :members:
```

```{eval-rst}
.. autoclass:: pangeo_forge.executors.PrefectPipelineExecutor
    :members:
```
