# Installing Pangeo Forge Recipes

## Getting Started - Binder

To begin experimenting with `pangeo-forge-recipes` the recommendation is to use Binder. There are links to open a Binder environment with JupyterLab in each tutorial.

## Local Installation

To install `pangeo-forge-recipes` on another machine the recommendation is to install using the CI environment. To do this:
1. `git clone -b 0.8.0 https://github.com/pangeo-forge/pangeo-forge-recipes.git`
1. `cd pangeo-forge-recipes`
2. `conda env create -n pangeo-forge-recipes python=3.9 --file=ci/py3.9.yml`

This will create a conda environment called `pangeo-forge-recipes` with the dependencies and a dev installation of the library `pangeo-forge-recipes` installed.
