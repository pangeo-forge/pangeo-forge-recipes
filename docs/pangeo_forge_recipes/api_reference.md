# API Reference

## File Patterns


```{eval-rst}
.. autoclass:: pangeo_forge_recipes.patterns.FilePattern
    :members:
    :special-members: __getitem__, __iter__
```

### Combine Dimensions

```{eval-rst}
.. autoclass:: pangeo_forge_recipes.patterns.ConcatDim
    :members:
```


```{eval-rst}
.. autoclass:: pangeo_forge_recipes.patterns.MergeDim
    :members:
```

### Indexing

```{eval-rst}
.. autoclass:: pangeo_forge_recipes.patterns.Index
    :members:
```

```{eval-rst}
.. autoclass:: pangeo_forge_recipes.patterns.DimKey
    :members:
```
```{eval-rst}
.. autoclass:: pangeo_forge_recipes.patterns.DimVal
    :members:
```

```{eval-rst}
.. autoclass:: pangeo_forge_recipes.patterns.CombineOp
    :members:
```

## Storage

```{eval-rst}
.. automodule:: pangeo_forge_recipes.storage
    :members:
```


## Processing Functions

The [Beam PTransform Style Guide](https://beam.apache.org/contribute/ptransform-style-guide/) recommends:

> Expose large, non-trivial, reusable sequential bits of the
> transform’s code, which others might want to reuse in ways you
> haven’t anticipated, as a regular function or class library.
> The transform should simply wire this logic together.

These are those functions.

```{eval-rst}
.. automodule:: pangeo_forge_recipes.openers
    :members:
```

```{eval-rst}
.. autoclass:: pangeo_forge_recipes.aggregation.XarrayCombineAccumulator
    :members:
    :special-members: __add__
```



## PTransforms

The [Beam PTransform Style Guide](https://beam.apache.org/contribute/ptransform-style-guide/) recommends:

> Expose every major data-parallel task accomplished by your
> library as a composite PTransform. This allows the structure of
> the transform to evolve transparently to the code that uses it.

```{eval-rst}
.. automodule:: pangeo_forge_recipes.transforms
    :members:
```

## Combiners


```{eval-rst}
.. automodule:: pangeo_forge_recipes.combiners
    :members:
```
