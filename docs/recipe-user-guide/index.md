# Pangeo Forge

Pangeo Forge is an open source tool for data Extraction, Transformation, and Loading (ETL).
The goal of Pangeo Forge is to make it easy to extract data from traditional data
repositories and deposit in cloud object storage in analysis-ready, cloud-optimized (ARCO) format.

Pangeo Forge is inspired by [Conda Forge](https://conda-forge.org/), a
community-led collection of recipes for building conda packages.
We hope that Pangeo Forge can play the same role for datasets.

## File Patterns

{doc}`file_patterns` are the starting point for any Pangeo Forge recipe.
they are the raw "ingredients" upon which the recipe will act.
The point of file patterns is to describe how many individual source files are
organized logically as part of a larger dataset.

## Recipes

The most important concept in Pangeo Forge is a {doc}``Recipe <recipes>``.
A recipe defines how to transform data in one format / location into another format / location.
The primary way people contribute to Pangeo Forge is by writing / maintaining recipes.
For information about how recipes work see {doc}`recipes`.
The {doc}`tutorials/index` provide deep dives into how to develop and debug Pangeo Forge Recipes.

## Recipe Execution

There are several different ways to execute Recipes.
See {doc}`execution` for details.

## Recipe Box

The {doc}`recipe_box` is a public database of Pangeo Forge Recipes.
Recipes are stored in Github repos.

## Bakeries

Bakeries are cloud-based environments for executing recipes from the Recipe Box.
Each Bakery is coupled to one or more cloud storage buckets where the ARCO data is stored.
Bakeries use [Prefect](https://prefect.io/) to orchestrate the various steps
of the recipe. For more information, see {doc}`bakeries`.

## Site Contents

```{toctree}
:maxdepth: 2

file_patterns
storage
recipes
execution
tutorials/index
recipe_box
bakeries
development/index
```
