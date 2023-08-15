# Installation

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
