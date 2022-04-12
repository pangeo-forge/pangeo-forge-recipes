import typing as t
import s3fs
import sys
import apache_beam as beam

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

BASE_PATH = 's3://esgf-world/CMIP6/OMIP/NOAA-GFDL/GFDL-CM4/omip1/r1i1p1f1/Omon/thetao/gr/v20180701/'


def run(pipeline_args: t.List[str]) -> None:
    # Define pattern
    fs = s3fs.S3FileSystem(anon=True)
    all_paths = fs.ls(BASE_PATH)
    pattern = pattern_from_file_sequence(['s3://' + path for path in all_paths], 'time')

    # Create Recipe
    rec = HDFReferenceRecipe(
        pattern,
        xarray_open_kwargs={"decode_coords": "all"},
        netcdf_storage_options={"anon": True}
    )

    with beam.Pipeline(argv=pipeline_args) as p:
        p | rec.to_beam()


if __name__ == '__main__':
    run(sys.argv[1:])

