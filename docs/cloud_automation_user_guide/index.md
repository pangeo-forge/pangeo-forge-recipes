# Cloud Automation User Guide

## Recipe Box

The {doc}`./recipe_box` is a public database of Pangeo Forge Recipes.
Recipes are stored in Github repos.

## Bakeries

Bakeries are cloud-based environments for executing recipes from the Recipe Box.
Each Bakery is coupled to one or more cloud storage buckets where the ARCO data is stored.
Bakeries use [Prefect](https://prefect.io/) to orchestrate the various steps
of the recipe. For more information, see {doc}`./bakeries`.


## Index

```{toctree}
:maxdepth: 1
:glob:

recipe_box
bakeries
```
