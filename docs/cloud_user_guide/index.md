# **Pangeo Forge Cloud** User Guide

Welcome to the User Guide for Pangeo Forge Cloud. While [Pangeo Forge Recipes](../recipe_user_guide/index.md) may be
{doc}`executed <../recipe_user_guide/execution>` on private infrastructure, the ultimate aim of Pangeo Forge
is to create a crowdsourced catalog of datasets, alongside the recipes used to build them, that is freely and publicly
accessible to all. This user guide explains core concepts for contributing to this public catalog.

## Core Concepts

[Pangeo Forge Recipes](../recipe_user_guide/index.md) produce [analysis-ready, cloud optimized (ARCO)](https://ieeexplore.ieee.org/abstract/document/9354557) datasets.
Recipes which have been run in {doc}`./bakeries` are listed on <https://pangeo-forge.org/catalog>. The {doc}`./recipes` page explains how to contribute recipes to this public catalog.


## Recipe Contribution

 how you can is a public database of Pangeo Forge Recipes.
Recipes are stored in Github repos, see {doc}`./recipes`.

## ChatOps Reference

, which are executed automatically in the cloud via {doc}`bakeries`.
Bakeries are cloud-based environments for executing recipes contributed by the community.
Each Bakery is coupled to one or more cloud storage buckets where the ARCO data produced by recipes is stored.
Bakeries use [Prefect](https://prefect.io/) to orchestrate the various steps
of the recipe. For more information, see {doc}`./bakeries`.


## Index

```{toctree}
:maxdepth: 1
:glob:

core_concepts
recipe_contribution
chatops_reference
```
