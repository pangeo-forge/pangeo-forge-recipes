# Recipe Composition

A recipe defines how to transform data in one format / location into another format / location.

A recipe is defined as a set of composite
[Apache Beam transforms](https://beam.apache.org/documentation/programming-guide/#transforms)
applied to the data collection associated with a {doc}`file pattern <file_patterns>`.

Specifically, a recipe is a set of transforms which operate on an `FilePattern` materialized in the form
of an [`apache_beam.PCollection`](https://beam.apache.org/documentation/programming-guide/#pcollections).

To write a recipe, you define a set of transforms using a combination of built-in Apache Beam transforms,
`pangeo-forge-recipes` transforms, and your own custom transforms for data pre-processing as needed.

```{note}
The full API Reference documentation for the existing recipe `PTransform` implementations ({class}`pangeo_forge_recipes.transforms`) can be found at
{doc}`../api_reference`.
```

## Deployment

There are several different ways to execute recipes. See {doc}`../deployment` for details.

## Index

```{toctree}
:maxdepth: 1
:glob:

recipes
file_patterns
openers
storage
examples/index
```
