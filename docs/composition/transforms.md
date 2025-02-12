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
how transforms are commonly connected together;
{doc}`examples/index` provides representative examples.
```

<div id=configurable-kwargs></div>

```{admonition} ⚙️ Deploy-time configurable keyword arguments
Keyword arguments designated by the gear emoji ⚙️ below are _deploy-time configurable_.
They should therefore _**not**_ be provided in your recipe file.
Instead, values for these arguments are specified in a per-deployment
[](../deployment/cli.md#configuration-file). The values provided in the configuration file
will be injected into your recipe by the {doc}`../deployment/cli`.
```

## Openers

Once you've created a {doc}`file pattern <file_patterns>` for your source data,
you'll need to open it somehow. Pangeo Forge currently provides the following openers:

- {class}`pangeo_forge_recipes.transforms.OpenURLWithFSSpec`
    - ⚙️ `cache` - <a href="#configurable-kwargs">Deploy-time configurable keyword argument</a>
- {class}`pangeo_forge_recipes.transforms.OpenWithXarray`

## Preprocessors

Before writing out your analysis-ready, cloud-optimized (ARCO) dataset, it's possible
you may want to preprocess the data. A custom Apache Beam `PTransform` can be written
for this purpose and included in your recipe.

```{code-cell}
# TODO: Add preprocessor example.
```

## Writers

- {class}`pangeo_forge_recipes.transforms.StoreToZarr`
    - ⚙️ `target_root` - <a href="#configurable-kwargs">Deploy-time configurable keyword argument</a>

## What's next

Once your recipe is defined, you're ready to move on to {doc}`../deployment/index`.
