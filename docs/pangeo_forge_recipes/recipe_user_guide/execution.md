# Recipe Execution

There are many different types of Pangeo Forge recipes.
However, **all recipes are executed the same way**!
This is a key part of the Pangeo Forge design.

Once you have created a recipe object (see {doc}`recipes`) you have two
options for executing it. In the subsequent code, we will assume that a
recipe has already been initialized in the variable `recipe`.


## Recipe Executors

A recipe is an abstract description of a transformation pipeline.
We currently support the following execution mechanism.

### Beam PTransform

A recipe is defined as a [pipeline](https://beam.apache.org/documentation/programming-guide/#creating-a-pipeline) of [Apache Beam transforms](https://beam.apache.org/documentation/programming-guide/#transforms) applied to the data collection associated with a {doc}`file pattern <file_patterns>`. Specifically, each recipe pipeline contains a set of transforms that operate on an `apache_beam.PCollection`, applying the specified transformation from input to output elements. Having created a transforms pipeline (see {doc}`recipes`}, it may be executed with Beam as follows:

```{code-block} python
import apache_beam as beam

with beam.Pipeline() as p:
    p | transforms
```

By default the pipeline runs using Beam's [DirectRunner](https://beam.apache.org/documentation/runners/direct/).
See [runners](https://beam.apache.org/documentation/#runners) for more details.
