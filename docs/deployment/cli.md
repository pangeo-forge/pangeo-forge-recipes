# Command Line Interface

## Prerequisites

Using the CLI to deploy a recipe assumes the following prerequisites:

1. The CLI is {doc}`installed <../getting_started/installation>`.
2. The recipe file resides with a {doc}`feedstock <feedstocks>`.
3. A [](#configuration-file) is available.

With these prerequistes complete, the CLI can be [invoked](#invocation) to deploy a recipe.

## Configuration file

`````{tab-set}
````{tab-item} Python

```{literalinclude} ../../examples/runner-config/local.py
```

````
````{tab-item} JSON

```{literalinclude} ../../examples/runner-config/local.json
---
language: json
---
```

````
`````

## Invocation

```{literalinclude} ../../examples/runner-commands/bake.sh
---
language: bash
---
```
