# Pangeo Forge Documentation

Resources for understanding and using Pangeo Forge

## First Steps

New to Pangeo Forge? You are in the right spot!

- {doc}`what_is_pangeo_forge` - Read more about Pangeo Forge and how it works.
- {doc}`introduction_tutorial/index` - Ready to code? Walk through creating, running, and staging your first Recipe.

## How the documentation is organized

There are a number of resources available when working with Pangeo Forge:

- **User Guides** explain core Pangeo Forge concepts in detail. They provide
  background information to aid in gaining a depth of understanding:
  - {doc}`recipe_user_guide/index` - For learning about how to create Recipes.
  - {doc}`cloud_automation_user_guide/index` - For digging deeper into the automation systems that
    power Pangeo Forge Cloud.
  - {doc}`development/index` - For developers seeking to contribute to Pangeo Forge core functionality.
- **{doc}`tutorials/index`** walk through examples of using Pangeo Forge Recipes. They are a good second step after the Introduction Tutorial.
- The **{doc}`api_reference`** is the complete technical documentation of all Pangeo Forge features.
  They are useful when you want to review a particular functionality in depth,
  but assumes you already have a working knowledge of the framework.

## Connecting with the Community

Pangeo Forge is a community run effort with a variety of roles:

- **Recipe contributors** — contributors who write recipes to define the data conversions. This can be anyone with a desire to create analysis ready cloud-optimized (ARCO) data.
- **Bakery operators** — individuals or instituations who deploy bakeries on cloud infrastructure to process and host the transformed data. This is typically an organization with a grant to fund the infrastructure.
- **Pangeo forge developers** - scientists and software developers who maintain and enhance the open-source code base which makes Pangeo Forge run. See {doc}`development/index`.

If you are new to Pangeo Forge and looking to get involved, we suggest starting with recipe contribution. You can do in two ways:

- Open a ticket with a dataset request (no code required!) - Explore other [recipe proposals](https://github.com/pangeo-forge/pangeo-forge-recipes/issues) and [create your own ticket](https://github.com/pangeo-forge/staged-recipes/issues/new/choose)
- Write a recipe for a dataset you'd like to see transformed - Get started with the {doc}`introduction_tutorial/index`


## Site Contents

```{toctree}
:maxdepth: 3

what_is_pangeo_forge
installation
introduction_tutorial/index
introduction_tutorial_ocean_sciences
recipe_user_guide/index
cloud_automation_user_guide/index
tutorials/index
api_reference
development/index
```
