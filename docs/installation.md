# Installation

## Local installation

To get set up Pangeo Forge you'll need to install the `pangeo-forge-recipes` library. The recommended way to do that is with `conda`.

```sh
conda install -c conda-forge pangeo-forge-recipes
```

If you have not added the conda forge channel or set it as your default channel and you would like to do so you can use the following commands:

```sh
conda config --add channels conda-forge
conda config --set channel_priority strict
```

If those steps are complete you can install the library with

```sh
conda install pangeo-forge-recipes
```

## Pangeo Forge Sandbox

Pangeo Forge also has a [code sandbox](https://github.com/pangeo-forge/sandbox) accessible online! The sandbox is an environment with `pangeo-forge-recipes` installed and template documents for creating a recipe. Links to both a Template workspace and a blank workspace with `pangeo-forge-recipes` installed are available in the [sandbox README](https://github.com/pangeo-forge/sandbox/blob/main/README.md).

You'll also notice that the tutorials have Binder links. The binder links can be used to explore `pangeo-forge-recipes` without setting up a local environment.
