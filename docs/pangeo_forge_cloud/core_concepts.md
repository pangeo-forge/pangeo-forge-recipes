# Core Concepts

When making a {doc}`../../pangeo_forge_cloud/recipe_contribution` to {doc}`../../pangeo_forge_cloud/index`, familiarity with the following core concepts is recommended.

## Catalog

The [analysis-ready, cloud optimized (ARCO)](https://ieeexplore.ieee.org/abstract/document/9354557) datasets produced by {doc}`../../pangeo_forge_recipes/recipe_user_guide/recipes`  executed on {doc}`../../pangeo_forge_cloud/index` are registered in the Pangeo Forge Catalog:

> <https://pangeo-forge.org/catalog>

Datasets in this catalog are built from recipes stored in [Feedstocks](#feedstocks).

## Feedstocks

Once a {doc}`../../pangeo_forge_cloud/recipe_contribution` passes the required [checks](../pangeo_forge_cloud/recipe_contribution.md#pr-checks), it is automatically moved into a new standalone GitHub repository within the [`pangeo-forge` Github organization](https://github.com/orgs/pangeo-forge/repositories). These repositories, which store production recipes, are known as **Feedstocks** (a concept [borrowed directly from Conda Forge](https://conda-forge.org/feedstock-outputs/)).

A current listing of all Pangeo Forge Feedstocks is available at:

> <https://pangeo-forge.org/dashboard/feedstocks>

All datasets in the [Catalog](#catalog) are built from recipes stored in a Feedstock repo. As such, Feedstocks serve as a record of the exact provenance of all ARCO data produced by {doc}`../../pangeo_forge_cloud/index`.

If corrections or improvements are required on a dataset in the [Catalog](#catalog), these can be implementeded via Pull Requests against the associated Feedstock repo. Each time a Pull Request is merged into the default branch of a Feedstock repo, a new production build of the recipes in that Feedstock is initiated.

Each build of the recipes in a Feedstock repo is tracked by a [Recipe Run](#recipe-runs).

## Recipe Runs

Recipe Runs are simple metadata objects used by {doc}`../../pangeo_forge_cloud/index` to track information about a particular recipe build.

A current listing of all Recipe Runs is available at:

> <https://pangeo-forge.org/dashboard/recipe-runs>

Every Recipe Run records metadata about a specific recipe execution job on a [Bakery](#bakeries).

## Bakeries

Bakeries are the cloud compute infrastructure that execute recipes contributed to
{doc}`../../pangeo_forge_cloud/index`. Bakeries use [Prefect](https://prefect.io/) to orchestrate the various stages of recipe execution. Each Bakery is coupled to one or more cloud storage buckets where the ARCO data produced by recipes is stored.

A listing of current Bakery deployments is available at:

> <https://pangeo-forge.org/dashboard/bakeries>

Bakery implementation templates are available for all three major cloud providers:

- <https://github.com/pangeo-forge/pangeo-forge-aws-bakery>
- <https://github.com/pangeo-forge/pangeo-forge-azure-bakery>
- <https://github.com/pangeo-forge/pangeo-forge-gcs-bakery>

Bakery operation entails non-trivial financial and maintenance cost, and is therefore typically undertaken by businesses, organizations, and/or institutions with dedicated funding for this purpose. If you are interested in deploying your own bakery, please consult the above-listed repos for more information.
