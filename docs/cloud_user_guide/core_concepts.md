# Core Concepts

## Catalog

[Pangeo Forge Recipes](../recipe_user_guide/index.md) produce [analysis-ready, cloud optimized (ARCO)](https://ieeexplore.ieee.org/abstract/document/9354557) datasets.
Recipes which have been run in {doc}`./bakeries` are listed on <https://pangeo-forge.org/catalog>. The {doc}`./recipes` page explains how to contribute recipes to this public catalog.

## Feedstocks



## Recipe Runs


## Bakeries

As described in {doc}`../recipe_user_guide/execution`, you can define and execute recipes on your
own computers using the executor of your choice.
However, you can also contribute your recipe to the Pangeo Forge {doc}`recipes`.
and have it executed automatically in the cloud.
Cloud based execution is provided by Bakeries.

<https://pangeo-forge.org/dashboard/bakeries>

There are currently three types of Bakeries implemented:

- <https://github.com/pangeo-forge/pangeo-forge-aws-bakery> -
  Bakery for Amazon Web Services cloud
- <https://github.com/pangeo-forge/pangeo-forge-azure-bakery> -
  Bakery for Microsoft Azure cloud
- <https://github.com/pangeo-forge/pangeo-forge-gcs-bakery> -
  Bakery for Google cloud


If you are interested in deploying your own bakery, please consult those repos
for more information.
