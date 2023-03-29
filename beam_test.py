import apache_beam as beam
import fsspec
import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithKerchunk,
    OpenWithXarray,
    StoreToZarr,
    WriteCombinedReference,
)

dates = pd.date_range("1981-09-01", "1981-09-03", freq="D")

URL_FORMAT = (
    "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/"
    "v2.1/access/avhrr/{time:%Y%m}/oisst-avhrr-v02r01.{time:%Y%m%d}.nc"
)


def make_url(time):
    return URL_FORMAT.format(time=time)


time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)
pattern = FilePattern(make_url, time_concat_dim)

from tempfile import TemporaryDirectory

td = TemporaryDirectory()
target_path = td.name + "/combined_reference"


transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithKerchunk(file_type=pattern.file_type)
    | beam.Map(print)
    # | WriteCombinedReference(concat_dims = ['time'], identical_dims = ['zlev','lat','lon'], target = target_path, reference_file_type='json')
)

with beam.Pipeline() as p:
    p | transforms
