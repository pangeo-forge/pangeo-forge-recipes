# Deployment

## Pure Beam

A recipe is defined as a [pipeline](https://beam.apache.org/documentation/programming-guide/#creating-a-pipeline) of [Apache Beam transforms](https://beam.apache.org/documentation/programming-guide/#transforms) applied to the data collection associated with a {doc}`file pattern <user_guide/file_patterns>`. Specifically, each recipe pipeline contains a set of transforms that operate on an `apache_beam.PCollection`, applying the specified transformation from input to output elements. Having created a transforms pipeline (see {doc}`user_guide/index`), it may be executed with Beam as follows:

```{code-block} python
import apache_beam as beam

with beam.Pipeline() as p:
    p | transforms
```

By default the pipeline runs using Beam's [DirectRunner](https://beam.apache.org/documentation/runners/direct/), which is useful during recipe development. However, alternative Beam runners are available, for example:
* [FlinkRunner](https://beam.apache.org/documentation/runners/flink/): execute Beam pipelines using [Apache Flink](https://flink.apache.org/).
* [DataflowRunner](https://beam.apache.org/documentation/runners/dataflow/): uses the [Google Cloud Dataflow managed service](https://cloud.google.com/dataflow/service/dataflow-service-desc).
* [DaskRunner](https://beam.apache.org/releases/pydoc/current/apache_beam.runners.dask.dask_runner.html): executes pipelines via [Dask.distributed](https://distributed.dask.org/en/stable/).

See [here](https://beam.apache.org/documentation/#runners) for details of the available Beam runners.

## `pangeo-forge` CLI

`````{tab-set}
````{tab-item} Python

```{literalinclude} ../examples/runner-config/local.py
```

````
````{tab-item} JSON

```{literalinclude} ../examples/runner-config/local.json
---
language: json
---
```

````
`````

```{literalinclude} ../examples/runner-commands/bake.sh
---
language: bash
---
```

## GitHub Action
