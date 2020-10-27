# Contribute a Recipe

To add a dataset to pangeo-forge you'll need to create a new recipe.

## Overview

1. [fork](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/fork-a-repo) the https://github.com/pangeo-forge/staged-recipes/ GitHub repository
2. Develop the pipeline (see below)
3. Submit the new recipe as a pull request

At this point the pangeo-forge maintainers (and [a host of bots](https://github.com/pangeo-bot)) will verify that your recipe is shipshape and ready for inclusion in pangeo-forge.
See [](#maintaining) for more on what happens next.

## Developing a Pipeline

Part of a pangeo-forge recipe is a python module `pipeline.py` that gives the code to transform raw data
to its analysis-ready form. We encorage you to build off the existing example in the pangeo-forge
repositories, including the [example-pipeline](https://github.com/pangeo-forge/staged-recipes/blob/master/recipes/example/pipeline.py).

Your `pipeline.py` file must contain a few things

1. A `Pipeline` class inheriting from `pangeo_forge.AbstractPipeline`
2. A Prefect `Flow` at the top-level of the module, `flow = Pipeline().flow`

The pangeo-forge bots will verify that everything is structured appropriately after you open
your pull request to [staged-recipes].

## Maintaining

Great, your pull request was accepted. Now what?


## Prefect for pangeo-forge maintainers

Here's a quick introduction to Prefect. Visit https://docs.prefect.io/core/ for a comprehensive
guide.

[staged-recipes]: https://github.com/pangeo-forge/staged-recipes/
