from pickle import dumps, loads

import pytest
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.openers import open_url
from pangeo_forge_recipes.patterns import FileType


@pytest.fixture(
    scope="module",
    params=[
        lazy_fixture("netcdf_local_paths_sequential_1d"),
        lazy_fixture("netcdf_http_paths_sequential_1d"),
    ],
    ids=["local", "http"],
)
def url_and_type(request):
    all_urls, _, _, _, extra_kwargs, type_str = request.param

    kwargs = {
        "secrets": extra_kwargs.get("query_string_secrets", None),
        "open_kwargs": extra_kwargs.get("fsspec_open_kwargs", None),
    }
    file_type = FileType(type_str)
    return all_urls[0], kwargs, file_type


@pytest.fixture(params=[True, False], ids=["with_cache", "no_cache"])
def cache(tmp_cache, request):
    if request.param:
        return tmp_cache
    else:
        return None


def test_open_url(url_and_type, cache):
    url, kwargs, file_type = url_and_type
    if cache:
        assert not cache.exists(url)
    open_file = open_url(url, cache=cache, **kwargs)
    open_file2 = loads(dumps(open_file))
    with open_file as f1:
        data = f1.read()
    with open_file2 as f2:
        data2 = f2.read()
    assert data == data2
    if cache:
        assert cache.exists(url)
        with cache.open(url, mode="rb") as f3:
            data3 = f3.read()
        assert data3 == data


# @pytest.mark.parametrize(
#     "file_paths",
#     [
#         lazy_fixture("netcdf_local_paths_sequential"),
#         lazy_fixture("netcdf_http_paths_sequential_1d"),
#     ],
# )
# @pytest.mark.parametrize("copy_to_local", [False, True])
# @pytest.mark.parametrize("use_cache, cache_first", [(False, False), (True, False), (True, True)])
# @pytest.mark.parametrize("use_dask", [True, False])
# @pytest.mark.parametrize("use_xarray", [True, False])
# def test_file_opener(
#     file_paths,
#     tmp_cache,
#     copy_to_local,
#     use_cache,
#     cache_first,
#     dask_cluster,
#     use_dask,
#     use_xarray,
# ):
#     all_paths = file_paths[0]
#     path = str(all_paths[0])
#     open_kwargs = file_paths[-1]["fsspec_open_kwargs"]
#     secrets = file_paths[-1]["query_string_secrets"]
#     cache = tmp_cache if use_cache else None
#
#     def do_actual_test():
#         if cache_first:
#             cache.cache_file(path, secrets, **open_kwargs)
#             assert cache.exists(path)
#             details = cache.fs.ls(cache.root_path, detail=True)
#             cache.cache_file(path, secrets, **open_kwargs)
#             # check that nothing happened
#             assert cache.fs.ls(cache.root_path, detail=True) == details
#
#         opener = file_opener(
#             path, cache, copy_to_local=copy_to_local, secrets=secrets, **open_kwargs
#         )
#         if use_cache and not cache_first:
#             with pytest.raises(FileNotFoundError):
#                 with opener as fp:
#                     pass
#         else:
#             with opener as fp:
#                 if copy_to_local:
#                     assert isinstance(fp, str)
#                     with open(fp, mode="rb") as fp2:
#                         if use_xarray:
#                             ds = xr.open_dataset(fp2, engine="h5netcdf")
#                             ds.load()
#                         else:
#                             _ = fp2.read()
#                 else:
#                     if use_xarray:
#                         ds = xr.open_dataset(fp, engine="h5netcdf")
#                         ds.load()
#                     else:
#                         _ = fp.read()
#                     assert hasattr(fp, "fs")  # should be true for fsspec.OpenFile objects
#
#     if use_dask:
#         with Client(dask_cluster) as client:
#             to_actual_test_delayed = delayed(do_actual_test)()
#             to_actual_test_delayed.compute()
#             client.close()
#     else:
#         do_actual_test()
