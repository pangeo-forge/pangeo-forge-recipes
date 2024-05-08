# import apache_beam as beam
# import fsspec
# import pandas as pd
# import zarr
# from apache_beam.runners.interactive.display import pipeline_graph
# from apache_beam.runners.interactive.interactive_runner import InteractiveRunner

# from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
# from pangeo_forge_recipes.storage import FSSpecTarget
# from pangeo_forge_recipes.transforms import (
#     OpenURLWithFSSpec,
#     OpenWithXarray,
#     StoreToPyramid,
#     StoreToZarr,
# )

# dates = pd.date_range("1981-09-01", "2022-02-01", freq="D")

# URL_FORMAT = (
#     "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/"
#     "v2.1/access/avhrr/{time:%Y%m}/oisst-avhrr-v02r01.{time:%Y%m%d}.nc"
# )


# fs = fsspec.get_filesystem_class("file")()
# path = str(".pyr_data")
# target_root = FSSpecTarget(fs, path)


# def make_url(time):
#     return URL_FORMAT.format(time=time)


# time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)
# pattern = FilePattern(make_url, time_concat_dim)

# pattern = pattern.prune()


# def test_ds(store: zarr.storage.FSStore) -> zarr.storage.FSStore:
#     # This fails integration test if not imported here
#     # TODO: see if --setup-file option for runner fixes this
#     import xarray as xr

#     ds = xr.open_dataset(store, engine="zarr", chunks={})
#     for var in ["anom", "err", "ice", "sst"]:
#         assert var in ds.data_vars
#     return store


# recipe = (
#     beam.Create(pattern.items()) | OpenURLWithFSSpec()
#     | OpenWithXarray(file_type=pattern.file_type)
# )


# pipeline = beam.Pipeline(InteractiveRunner())

# with pipeline as p:
#     process = p | recipe
#     base_store = process | "Write Base Level" >> StoreToZarr(
#         target_root=target_root,
#         store_name="store",
#         combine_dims=pattern.combine_dim_keys,
#     )
#     pyramid_store = process | "Write Pyramid Levels" >> StoreToPyramid(
#         target_root=target_root,
#         store_name="pyramid",
#         epsg_code="4326",
#         rename_spatial_dims={"lon": "longitude", "lat": "latitude"},
#         levels=4,
#         pyramid_kwargs={"extra_dim": "zlev"},
#         combine_dims=pattern.combine_dim_keys,
#     )

# print(pipeline_graph.PipelineGraph(pipeline).get_dot())
