from apache_beam.options.pipeline_options import PipelineOptions


class PangeoForgeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--target_url",
            type=str,
            help="Where to store the pipeline outputs. Passed to fsspec.",
            # required=True
        )
        parser.add_value_provider_argument(
            "--cache_url",
            type=str,
            help="Directory where to cache input files. Passed to fsspec",
            # required=True
        )
