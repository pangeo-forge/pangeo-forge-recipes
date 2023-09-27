---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Transforms

Once you have a {doc}`file pattern <file_patterns>` for your source data, it's time to define
a set of transforms to apply to the data, which may include:

  - Standard transforms from Apache Beam's
    [Python transform catalog](https://beam.apache.org/documentation/transforms/python/overview/)
  - `pangeo-forge-recipes` core transforms, such as [](#openers) and [](#writers)
  - Third-party extensions from the Pangeo Forge {doc}`../ecosystem`
  - Your own transforms, such as custom [](#preprocessors)

```{hint}
Please refer to the [](./index.md#generic-sequence) and [](./styles.md) for discussion of
how transforms are commonly connected together.
```

## Openers

Once you've created a {doc}`file pattern <file_patterns>` for your source data,
you'll need to open it somehow. Pangeo Forge currently provides the following openers:

- {class}`pangeo_forge_recipes.transforms.OpenURLWithFSSpec`
- {class}`pangeo_forge_recipes.transforms.OpenWithXarray`
- {class}`pangeo_forge_recipes.transforms.OpenWithKerchunk`

## Preprocessors

Before writing out your analysis-ready, cloud-optimized (ARCO) dataset, it's possible
you may want to preprocess the data. A custom Apache Beam `PTransform` can be written
for this purpose and included in your recipe.

```{code-cell}
# TODO: Add preprocessor example.
```

## Writers

- {class}`pangeo_forge_recipes.transforms.StoreToZarr`
- {class}`pangeo_forge_recipes.transforms.WriteCombinedReference`

## What's next

Once your recipe is defined, you're ready to move on to {doc}`../deployment/index`.
