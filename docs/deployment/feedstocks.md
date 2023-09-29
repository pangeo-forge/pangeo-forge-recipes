# From recipe to feedstock

## What is a feedstock?

A feedstock is a code repository containing a Pangeo Forge recipe along with associated metadata files.

## Directory structure

The feedstock repo should contain a subdirectory named `feedstock`,
which contains at minimum the following three files:

```
.
└── feedstock
    ├── meta.yaml
    ├── recipe.py
    └── requirements.txt
```

The `recipe.py` file is the recipe itself
(see {doc}`../composition/index` for how to create this file).
The other two files are described below.

## `meta.yaml`

At minimum, this file requires a `recipes` section with `id` and
`object` fields:

```yaml
# meta.yaml

recipes:
  - id: "unique-recipe-id"
    object: "recipe_module_name:recipe_object_name"
```

The `id` field is a unique identifier for your recipe, and can be any
descriptive or memorable name of your choosing.

The `object` field records the _name of the Python module_ which
contains your recipe and the _name of the recipe (i.e., transforms)
object within that module_ to deploy.

```{note}
While a `recipes` section is the minimum requirement for a `meta.yaml`,
this file is also intended to contain additional provenance information about the
dataset. Documentation of this aspect is pending implementation of a schema
for these fields: <https://github.com/pangeo-forge/pangeo-forge-runner/issues/93>.
```

```
# TODO: Document `dict_object` usage pattern.
```

## `requirements.txt`

This file should contain the list of dependencies required
to run your recipe, including `pangeo-forge-recipes`, and should follow
[`pip`'s requirements file format](https://pip.pypa.io/en/stable/reference/requirements-file-format/).

It is advisable for all packages listed here to be pinned to a specific version, which is beneficial
for reproducible deployments. For example:

```
# requirements.txt

pangeo-forge-recipes==<version>
```
