# From recipe to feedstock

## What is a feedstock?

A feedstock is a special type of repository (i.e., directory) used for
housing Pangeo Forge recipes. By adhering to a particular [structure](#directory-structure), the feedstock repository

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

```yaml
# meta.yaml

recipes:
  - id: "unique-recipe-id"
    object: "recipe_module_name:recipe_object_name"
```

```{note}
TODO: explain meta.yaml schema
```

## `requirements.txt`

This file should contain the list of pinned dependencies required
to run your recipe, including `pangeo-forge-recipes`.

```
# requirements.txt

pangeo-forge-recipes==0.10.2
```
