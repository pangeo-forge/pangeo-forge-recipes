# Pangeo Forge Documentation

Pangeo Forge is an open source framework for Extraction, Transformation, and Loading (ETL) of scientific data.

## First Steps

New to Pangeo Forge? You are in the right spot!

- {doc}`what_is_pangeo_forge` - Read more about Pangeo Forge and how it works.
- {doc}`introduction_tutorial/index` - Ready to code? Walk through creating, running, and staging your first Recipe.

## How the documentation is organized

There are a number of resources available when working with Pangeo Forge:

- **Introduction Tutorial**: {doc}`introduction_tutorial/index` - Walks you through creating, running, and staging your first Recipe.
- **User Guides** explain core Pangeo Forge concepts in detail. They provide
  background information to aid in gaining a depth of understanding:
  - {doc}`pangeo_forge_recipes/recipe_user_guide/index` - For learning about how to create Recipes. A recipe is defined as a [pipeline](https://beam.apache.org/documentation/programming-guide/#creating-a-pipeline) of [Apache Beam](https://beam.apache.org/) [transforms](https://beam.apache.org/documentation/programming-guide/#transforms) applied to a data collection, performing one or more transformations of input elements to output elements.
  - {doc}`development/development_guide` - For developers seeking to contribute to Pangeo Forge core functionality.
- **Advanced Examples** walk through examples of using Pangeo Forge Recipes:
  - {doc}`tutorials/index` - Are in-depth demonstrations of using Pangeo Forge Recipes with real world datasets. They are a good next step after the Introduction Tutorial.
- **References** are the complete technical documentation of Pangeo Forge features.  They are useful when you want to review a particular functionality in depth,
but assume you already have a working knowledge of the framework:
  - {doc}`api_reference`
  - {doc}`pr_checks_reference`


## Connecting with the Community

Pangeo Forge is a community run effort with a variety of roles:

- **Recipe contributors** — contributors who write recipes to define the data conversions. This can be anyone with a desire to create analysis ready cloud-optimized (ARCO) data. To get involved, see {doc}`pangeo_forge_cloud/recipe_contribution`.
- **Bakery operators** — individuals or instituations who deploy bakeries on cloud infrastructure to process and host the transformed data. This is typically an organization with a grant to fund the infrastructure. For more information, see the Bakeries section of {doc}`pangeo_forge_cloud/core_concepts`.
- **Pangeo Forge developers** - scientists and software developers who maintain and enhance the open-source code base which makes Pangeo Forge run. See {doc}`pangeo_forge_recipes/development/index`.

If you are new to Pangeo Forge and looking to get involved, we suggest starting with  {doc}`pangeo_forge_cloud/recipe_contribution`.


## Site Contents

```{toctree}
:maxdepth: 2

what_is_pangeo_forge
introduction_tutorial/index
installation
user_guide/index
examples/index
api_reference
development/index
ecosystem
```
