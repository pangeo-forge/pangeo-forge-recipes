# User Guide

Welcome to the User Guide for the `pangeo-forge-recipes` Python package.

## File Patterns

{doc}`file_patterns` are the starting point for any Pangeo Forge recipe.
they are the raw "ingredients" upon which the recipe will act.
The point of file patterns is to describe how many individual source files are
organized logically as part of a larger dataset.

## Recipe Object

The central object in `pangeo_forge_recipes` is a {doc}``Recipe Object <recipes>``.
A Recipe Object defines how to transform data in one format / location into another format / location.
The primary way people contribute to Pangeo Forge is by writing / maintaining Recipes.
For information about how recipes work see {doc}`recipes`.
The {doc}`../tutorials/index` provide deep dives into how to develop and debug Recipes.

## Recipe Execution

There are several different ways to execute Recipes.
See {doc}`execution` for details.

## Index

```{toctree}
:maxdepth: 1
:glob:

file_patterns
recipes
storage
execution
```
