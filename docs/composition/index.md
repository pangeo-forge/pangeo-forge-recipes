# Recipe Composition

A recipe describes the steps to transform archival source data in one
format / location into analysis-ready, cloud-optimized (ARCO) data in another format /
location. Technically, a recipe is as a set of composite
[Apache Beam transforms](https://beam.apache.org/documentation/programming-guide/#transforms)
applied to the data collection associated with a {doc}`file pattern <file_patterns>`.
To write a recipe:

1. Define a {doc}`file pattern <file_patterns>` for your source data.
2. Define a set of transforms to apply to the source data, using a combination of:
    - Standard transforms from Apache Beam's
      [Python transform catalog](https://beam.apache.org/documentation/transforms/python/overview/)
    - `pangeo-forge-recipes` core transforms, such as {doc}`openers` and {doc}`writers`
    - Third-party extensions from the Pangeo Forge {doc}`../ecosystem`
    - Your own transforms, such as custom {doc}`preprocess`
3. Put all of this code into a Python module (i.e., a file with `.py` extension). See {doc}`examples/index`.


```{note}
API Reference for the core {class}`pangeo_forge_recipes.transforms`
can be found in {doc}`../api_reference`.
```

## Index

```{toctree}
:maxdepth: 1
:glob:

file_patterns
openers
preprocess
writers
examples/index
```
