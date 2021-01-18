# Concepts

pangeo-forge is modeled after [conda-forge], a community-led collection of recipes
for building conda packages.

## Recipes

The most important concept in Pangeo Forge is a ``Recipe``.
A recipe defines how to transform data in one format / location into another format / location.


## Storage




## Components

The high-level components of pangeo-forge are:

* https://github.com/pangeo-forge/staged-recipes: The birthplace for new recipes.
  Anyone interested in adding a dataset to pangeo-forge can submit a new recipe
  through a pull request, using the examples there as a starting point.
* Recipes: Metadata describing a dataset and code for transforming it from
  raw input to analysis-ready dataset.
* pangeo-forge: A Python library containing useful pipeline helpers and the
  [pangeo-forge command-line-interface][cli] for validating pipelines.
* https://github.com/pangeo-forge/docker-images: A repository for building docker
  images for pangeo-forge.

## Pipeline Structure

pangeo-forge uses [Prefect] for

1. Pipeline definitions: the code that expresses the transformation from raw to analysis-ready data.
2. Orchestration: everything involved with taking a pipeline definition and actually running it.

This imposes a few constraints which we build upon.

All recipe definitions must be named `recipe/pipeline.py` and must contain an instances
of a `prefect.Flow` at the top-level of the module.

Additionally, pangeo-forge imposes additional strucutre. All pipelines modules should include a `Pipeline`
class that inherits from `pangeo_forge.AbstractPipeline`. So most pipeline modules will have


```
class Pipeline(pangeo_forge.AbstractPipeline):
    @property
    def flow(self):
        with prefect.Flow(self.name) as flow:
            ...
        return flow


pipeline = Pipeline()
flow = pipeline.flow
```

## Lifecycle of a recipe

A maintainer contributes a recipe to [staged-recipes] through a pull request. We
use GitHub Actions to perform some initial validation. The actions are at https://github.com/pangeo-forge/staged-recipes/blob/master/.github/workflows/main.yaml and the logs are available at https://github.com/pangeo-forge/staged-recipes/actions.

When a PR is merged into `staged-recipes` the [Create Repository](https://github.com/pangeo-forge/staged-recipes/blob/master/.github/workflows/create-repository.yaml) action is run, which

1. Creates a new git repository with the contents of `staged-recipes/examples/<new-pipeline>`
   inserted, along with a few other files provided by pangeo-forge, including the `register_pipeline` action definition.
2. Pushes that new repository to `https://pangeo-forge/<new-pipeline>`, for example https://github.com/pangeo-forge/example-pipeline/.
3. Executes the [Register Pipeline](https://github.com/pangeo-forge/staged-recipes/blob/master/.github/workflows/scripts/register_pipeline.yaml) action, which registers the new pipeline with Prefect and executes it.

[conda-forge]: https://conda-forge.github.io
[cli]: https://github.com/pangeo-forge/pangeo-forge/blob/master/pangeo_forge/cli.py
[Prefect]: https://docs.prefect.io
[staged-recipes]: https://github.com/pangeo-forge/staged-recipes/
