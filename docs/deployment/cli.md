# Command Line Interface

## Advantages



## Prerequisites

Using command line interface (CLI) to deploy a recipe assumes the following prerequisites:

1. The CLI is installed:

    ```
    pip install pangeo-forge-runner
    ```
2. The recipe file resides within a particular [](#directory-structure),
   alongside [](#metayaml) and [](#requirementstxt) files.
3. A [](#configuration-file) is available.

With these prerequistes complete, the CLI can be [invoked](#invocation) to deploy a recipe.


## Directory structure

```
.
└── feedstock
    ├── meta.yaml
    ├── recipe.py
    └── requirements.txt
```

## `meta.yaml`

```yaml
recipes:
  - id: "gpcp-from-gcs"
    object: "gpcp_from_gcs:recipe"
```

## `requirements.txt`

...

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
