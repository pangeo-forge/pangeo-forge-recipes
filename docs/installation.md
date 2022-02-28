# Installing Pangeo Forge Recipes

## Local installation

To get set up Pangeo Forge you'll need to install the `pangeo-forge-recipes` library. The recommended way to do that is with `conda`.

`conda install -c conda-forge pangeo-forge-recipes`

If you have not added the conda forge channel or set it as your default channel and you would liek to do so you can use the following commands:

`conda config --add channels conda-forge`
`conda config --set channel_priority strict`

If those steps are complete you can install the library with `conda install pangeo-forge-recipes`.

## Remote access

Pangeo Forge also has a [code sandbox](https://github.com/pangeo-forge/sandbox) which has `pangeo-forge-recipes` installed and template documents for creatig a recipe. Click the button below to launch the sandbox using Binder:

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/pangeo-forge/sandbox/binder)

You'll also notice that the tutorials have Binder links. The binder links can be used to explore `pangeo-forge-recipes` without setting up a local environment.
