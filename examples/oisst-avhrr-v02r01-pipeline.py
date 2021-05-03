from pangeo_forge_recipes.pipeline import AbstractPipeline, XarrayPrefectPipelineMixin


class Pipeline(AbstractPipeline, XarrayPrefectPipelineMixin):
    def __init__(self, name="oisst-avhrr-v02r01"):
        self.name = name
        self.cache_location = f"gs://pangeo-scratch/rabernat/pangeo_smithy/{name}-cache/"
        self.concat_dim = "time"
        self.files_per_chunk = 5

    @property
    def sources(self):
        import pandas as pd

        keys = pd.date_range("1981-09-01", "1981-09-10", freq="D")
        source_url_pattern = (
            "https://www.ncei.noaa.gov/data/"
            "sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/"
            "{yyyymm}/oisst-avhrr-v02r01.{yyyymmdd}.nc"
        )
        source_urls = [
            source_url_pattern.format(yyyymm=key.strftime("%Y%m"), yyyymmdd=key.strftime("%Y%m%d"))
            for key in keys
        ]
        return source_urls

    @property
    def targets(self):
        return f"gs://pangeo-scratch/rabernat/pangeo_smithy/{self.name}-target/"
