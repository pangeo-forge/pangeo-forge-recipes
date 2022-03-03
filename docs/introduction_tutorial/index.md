# Introduction Tutorial

Welcome to the Pangeo Forge introduction tutorial!

### The Structure
This tutorial is split into three parts:

1. Defining a `FilePattern`
2. Defining a recipe and running it locally
3. Setting up a recipe to run in the cloud

The first two parts use **`pangeo_forge_recipes`**, the Python library for defining dataset conversions. In the third part we switch to utilize **Pangeo Forge Cloud**, and organize our code for using scaled cloud infrastructure. These two pieces - **`pangeo_forge_recipes`** and **Pangeo Forge Cloud** can be used seperately, but are highly related.

## The Goal
Throughout this tutorial we are going to convert NOAA OISST stored in netCDF to Zarr. OISST is a global, gridded ocean sea surface temperature dataset at daily 1/4 degree resolution. By the end of this tutorial sequence you will have converted some OISST data to zarr, be able to access a sample on your computer, and see how to propose the recipe for cloud deployment!

## The Setup
The introduction tutorial assumes that you already have `pangeo-forge-recipes` installed (See {doc}`/installation`). Alternately, you can run these tutorials in [the Pangeo Forge Sandbox](https://mybinder.org/v2/gh/pangeo-forge/sandbox/binder?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fpangeo-forge%252Fsandbox%26urlpath%3Dlab%252Ftree%252Fsandbox%252Fscratch.ipynb%26branch%3Dmain).

There are pros and cons to both the local development environment and the sandbox environment. The sandbox is quick to setup (just click the link!) but the files created there have to be downloaded if you want to save them forever. Local development takes longer to setup, but once its set up you have a permanent place to build recipes.

For the tutorial feel free to pick the environment that makes the most sense to you -- local development if you want to be set up for the long term, or the sandbox if you just can't wait to get started!

## Index

```{toctree}
:maxdepth: 1

intro_tutorial_part1
intro_tutorial_part2
intro_tutorial_part3
```
