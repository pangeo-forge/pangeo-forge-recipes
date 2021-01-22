# Pangeo Forge

Pangeo Forge is an open source tool for data Extraction, Transformation, and Loading (ETL).
The goal of Pangeo Forge is to make it easy to extract data from traditional data
repositories and deposit in cloud object storage in analysis-ready, cloud-optimize (ARCO) format.

Pangeo Forge is inspired by [Conda Forge](https://conda-forge.org/), a
community-led collection of recipes for building conda packages.
We hope that Pangeo Forge can play the same role for datasets.

## Recipes

The most important concept in Pangeo Forge is a ``recipe``.
A recipe defines how to transform data in one format / location into another format / location.
The primary way people contribute to Pangeo Forge is by writing / maintaining recipes.
Recipes developed by the community are stored in GitHub repositories.
For information about how recipes work see {doc}`recipes`.
The {doc}`tutorials/index` provide deep dives into how to develop and debug Pangeo Forge recipes.

## Recipe Execution

There are several different ways to execute recipes.
See {doc}`execution` for details.

## Bakeries

Bakeries are cloud-based environments for executing recipes.
Each Bakery is coupled to one or more cloud storage buckets where the ARCO data is stored.
Bakeries use [Prefect](https://prefect.io/) to orchestrate the various steps
of the recipe.
For more information, see {doc}`bakeries`.


```{toctree}
:maxdepth: 2
:caption: Contents

recipes
tutorials/index
execution
bakeries
contribute
api

```
