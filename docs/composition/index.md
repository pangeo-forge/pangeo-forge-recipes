# Recipe Composition

## Overview

A recipe describes the steps to transform archival source data in one
format / location into analysis-ready, cloud-optimized (ARCO) data in another format /
location. Technically, a recipe is a composite of
[Apache Beam transforms](https://beam.apache.org/documentation/programming-guide/#transforms)
applied to the data collection associated with a {doc}`file pattern <file_patterns>`.
To write a recipe:

1. Define a {doc}`file pattern <file_patterns>` for your source data.
2. Select a set of {doc}`transforms` to apply to the source data.
3. Put all of this code into a Python module (i.e., a file with `.py` extension),
   as demonstrated in {doc}`examples/index`.

## Generic sequence

Most recipes will be composed following the generic sequence:

**{doc}`FilePattern <file_patterns>`**
**`|` [Opener](./transforms.md#openers)**
**`|` [Preprocessor](./transforms.md#preprocessors) (Optional)**
**`|` [Writer](./transforms.md#writers)**

```{tip}
In Apache Beam, transforms are connected with the `|` pipe operator.
```

Or, in pseudocode:

```python
recipe = (
    beam.Create(pattern.items())
    | Opener
    | Preprocessor  # optional
    | Writer
)
```

Pangeo Forge does not provide any importable, pre-defined sequences of transforms. This is by design,
and leaves the composition process flexible enough to accomodate the heterogeneity of real world data. In
practice, however, certain {doc}`styles` may work as the basis for many datasets.

## Index

```{toctree}
:maxdepth: 1
:glob:

file_patterns
transforms
styles
examples/index
```
