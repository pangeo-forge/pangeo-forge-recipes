# Deployment

## Beam Runners

A recipe is defined as a [pipeline](https://beam.apache.org/documentation/programming-guide/#creating-a-pipeline) of [Apache Beam transforms](https://beam.apache.org/documentation/programming-guide/#transforms) applied to the data collection associated with a {doc}`file pattern <../composition/file_patterns>`. Specifically, each recipe pipeline contains a set of transforms that operate on an `apache_beam.PCollection`, applying the specified transformation from input to output elements. Having created a transforms pipeline (see {doc}`../composition/index`), it may be executed with Beam as follows:

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

## Advantages

While recipes can be deployed with Pure Beam, using the Pangeo Forge
command line interface (CLI) provides the following advantages:

- Centralized configuration
- Sensible defaults
- Deploy from version control refs

```{toctree}
:maxdepth: 1

feedstocks
cli
action
example-feedstocks
```
