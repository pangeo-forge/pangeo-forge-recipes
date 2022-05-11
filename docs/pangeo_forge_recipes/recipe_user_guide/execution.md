# Recipe Execution

There are many different types of Pangeo Forge recipes.
However, **all recipes are executed the same way**!
This is a key part of the Pangeo Forge design.

Once you have created a recipe object (see {doc}`recipes`) you have two
options for executing it. In the subsequent code, we will assume that a
recipe has already been initialized in the variable `recipe`.


## Recipe Executors

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

```{warning}
For large recipes, this default can lead to Prefect Flows with >10000 Tasks.
In our experience, Prefect can struggle with this volume.
```

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
