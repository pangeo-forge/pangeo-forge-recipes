# Deployment

```{warning}
The CLI of Pangeo Forge (pangeo-forge-runner) is currently unmaintained. It is recommended to use the beam pipeline deployment logic below instead.
```

## Advantages of the CLI

The {doc}`cli` (CLI) is the recommended way to deploy Pangeo Forge {doc}`recipes <../composition/index>`,
both for production and local testing. Advantages of using the CLI include:

- Centralized configuration
- Sensible defaults
- Deploy from version control refs

The CLI is itself a thin wrapper around Apache Beam's pipeline deployment logic (in pseudocode):

```{code-block} python
import apache_beam as beam
from apache_beam.pipeline import PipelineOptions

options = PipelineOptions(runner="DirectRunner", ...)

with beam.Pipeline(options=options) as p:
    p | recipe
```

Users are welcome to use this native Beam deployment approach for their recipes as well.

## Beam Runners

Apache Beam (and therefore, Pangeo Forge) supports flexible deployment via "runners",
which include:

- [DirectRunner](https://beam.apache.org/documentation/runners/direct/):
  Useful for testing during recipe development and, in multithreaded mode, for certain production workloads.
  (Note that Apache Beam does _not_ recommend this runner for production.)
- [FlinkRunner](https://beam.apache.org/documentation/runners/flink/):
  Executes pipelines using [Apache Flink](https://flink.apache.org/).
- [DataflowRunner](https://beam.apache.org/documentation/runners/dataflow/):
  Uses the [Google Cloud Dataflow managed service](https://cloud.google.com/dataflow/service/dataflow-service-desc).
- [DaskRunner](https://beam.apache.org/releases/pydoc/current/apache_beam.runners.dask.dask_runner.html):
  Executes pipelines via [Dask.distributed](https://distributed.dask.org/en/stable/).

When deploying with the CLI, the runner is specified via a [](cli.md#configuration-file).

## Index

```{toctree}
:maxdepth: 1

feedstocks
cli
action
example-feedstocks
```
