# Pangeo Forge Documentation

Resources for understanding and using Pangeo Forge

## First Steps

New to Pangeo Forge? Start here!

- {doc}`what_is_pangeo_forge` - Read more about Pangeo Forge and how it works!
- {doc}`intro_tutorial` - Ready to code? Walk through creating and deploying your first Recipe.

## How the documentation is organized

There are a number of places to access resources when working with components of Pangeo Forge.
Here is an overview of what you will find:

- The {doc}`intro_tutorial` is the place to start with Pangeo Forge.
  It walks the user through the process of getting set up with their first Recipe.
- The **User Guides** explain core Pangeo Forge concepts in detail. They provide
  background information to aid in gaining a depth of understanding:
  - {doc}`recipe_user_guide/index` - For learning about how to create Recipes.
  - {doc}`development/index` - For developers seeking to contribute to Pangeo Forge core functionality.
  - {doc}`cloud_automation_user_guide/index` - For digging deeper into the automation systems that
    power Pangeo Forge in the cloud.
- **Reference Pages** are the complete technical documentation of all Pangeo Forge features.
  They are useful when you want to review a particular functionality in depth,
  but assume you already have a working knowledge of the code base

## Repository Reference

There are many respositories that make up Pangeo Forge. Here are links to the different documentation pages:

- pangeo-forge-recipes
- pangeo-forge-azure-bakery
- pangeo-forge-aws-bakery

## Connecting the Community

Pangeo Forge is a community run effort. There are different roles that people play to support the effort:

- Recipe contributors — contributors who write recipes to define the data conversions. This can be anyone with a desire to create analysis ready cloud-optimized (ARCO) data
- Bakery operators — individuals or instituations who deploy bakeries on cloud infrastructure to process and host the transformed data. This is typically an organization with a grant to fund the infrastructure
- Pangeo forge developers - scientists and software developers who contribute to maintaining and enhancing the open-source code base which makes Pangeo Forge run.

If you are new to Pangeo Forge and looking to get involved, we suggest getting started with recipe contribution. You can do in two ways:

- Open a ticket with a dataset request (no code required!) - Get started here (link)
- Write a recipe for a dataset you'd like to see transformed - See recipe creation docs


## Site Contents

```{toctree}
:maxdepth: 2

what_is_pangeo_forge
introduction_tutorial/index
recipe_user_guide/index
cloud_automation_user_guide/index
tutorials/index
development/index
```
