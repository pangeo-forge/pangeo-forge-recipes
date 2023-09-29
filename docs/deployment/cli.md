# Command Line Interface

## Prerequisites

Using the CLI to deploy a recipe assumes the following prerequisites:

1. The CLI is {doc}`installed <../getting_started/installation>`.
2. The recipe file resides with a {doc}`feedstock <feedstocks>`.
3. A [](#configuration-file) is available.

With these prerequistes complete, the CLI can be [invoked](#invocation) to deploy a recipe.

## Configuration file

Deployment requires a configuration file which can be provided as a
Python or JSON file, e.g.:

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

This file centralizes configuration of:

- Apache Beam [Pipeline Options](https://beam.apache.org/documentation/programming-guide/#configuring-pipeline-options)
- <a href="../composition/transforms.html#configurable-kwargs">Deploy-time configurable keyword arguments</a>

For a full listing of available configuration, see:
[Configuration Reference](https://pangeo-forge-runner.readthedocs.io/en/latest/reference/index.html).

## Invocation

```{literalinclude} ../../examples/runner-commands/bake.sh
---
language: bash
---
```
Where the variables have the following values assigned to them:

- `REPO`: Path to the feedstock repo. This can be a
local path or a URL to a GitHub repo.
- `CONFIG_FILE`: Local path to the deployment [](#configuration-file).
- `RECIPE_ID`: The `id` of the recipe you'd like to run as it appears
in your feedstock's [](./feedstocks.md#metayaml).
- `JOB_NAME`: A unique name for this deployment.

```{tip}
The `--prune` flag limits the deployment to the first two increments
of the dataset's concat dimension. This is useful for testing your
deployment on a limited subset of data. To deploy a full production
run, simply omit this flag.
```
