# Introduction Tutorial

Welcome to the Pangeo Forge introduction tutorial!

## The Structure
This tutorial is split into three parts:

1. {doc}`part1`
2. {doc}`intro_tutorial_part2`
3. {doc}`intro_tutorial_part3`

The first two parts use {doc}`../pangeo_forge_recipes/index`, the Python library for defining dataset conversions. In the third part we switch to utilize {doc}`../pangeo_forge_cloud/index`, and organize our code for using scaled cloud infrastructure. These two pieces can be used seperately, but are highly related.

## The Goal
Throughout this tutorial we are going to convert NOAA OISST stored in netCDF to Zarr. OISST is a global, gridded ocean sea surface temperature dataset at daily 1/4 degree resolution. By the end of this tutorial sequence you will have converted some OISST data to zarr, be able to access a sample on your computer, and see how to propose the recipe for cloud deployment!

## The Setup
The introduction tutorial assumes that you already have `pangeo-forge-recipes` installed (See {doc}`../pangeo_forge_recipes/installation`). Alternately, you can run these tutorials in [the Pangeo Forge Sandbox](https://mybinder.org/v2/gh/pangeo-forge/sandbox/binder?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fpangeo-forge%252Fsandbox%26urlpath%3Dlab%252Ftree%252Fsandbox%252Fscratch.ipynb%26branch%3Dmain).

There are pros and cons to both the local development environment and the sandbox environment. The sandbox is quick to setup (just click the link!) but the files created there have to be downloaded if you want to save them forever. Local development takes longer to setup, but once its set up you have a permanent place to build recipes.

For the tutorial feel free to pick the environment that makes the most sense to you -- local development if you want to be set up for the long term, or the sandbox if you just can't wait to get started!

## Index

```{toctree}
:maxdepth: 1

part1
intro_tutorial_part2
intro_tutorial_part3
```
