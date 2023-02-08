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
