# Recipes

A recipe defines how to transform data in one format / location into another format / location.
The primary way people contribute to Pangeo Forge is by writing / maintaining recipes.

```{warning}
The Recipe API is still in flux and may change. Make sure the version of the documentation
you are reading matches your installed version of pangeo_forge.
```

## The Recipe Object

The job of a Recipe is to take a file pattern and turn into a single analysis-ready, cloud-optimized Dataset.
For the common scenario of assembling many NetCDF files into a single Zarr
group, we use {class}`pangeo_forge.recipes.XarrayZarrRecipe`.

Initializing a recipe looks something like this.

```{code-block} python
file_pattern = pattern_from_file_sequence(...)
recipe = XarrayZarrRecipe(file_pattern)
```

There are many other options we can pass, all covered in the API documentation for {class}`pangeo_forge.recipes.XarrayZarrRecipe`.
For a deeper dive on how to pick these options and what they mean, check out the
tutorial: {doc}`tutorials/netcdf_zarr_sequential`.

Your recipe will also need {doc}`storage <storage>`.
If you have already defined a `Target` object,
then you can either assign it when you initialize the recipe or later, e.g.

```{code-block} python
recipe.target = FSSpecTarget(fs=fs, root_path=target_path)
```

This particular class of recipe also requires a cache, a place to store temporary
files. We can create one as follows.

```{code-block} python
recipe.input_cache = CacheFSSpecTarget(fs=fs, root_path=cache_path)
```

Once your recipe is defined and has its targets assigned, you're ready to
move on to {doc}`execution`.

## Recipe API

```{eval-rst}
.. autoclass:: pangeo_forge.recipes.XarrayZarrRecipe
```
