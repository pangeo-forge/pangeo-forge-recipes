import dask.core
import pytest
from dask.highlevelgraph import HighLevelGraph

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe

# TODO: revise this test once refactor is farther along
pytest.skip(allow_module_level=True)


def _make_filename_for_memory_usage_test(time):
    import pandas as pd

    input_url_pattern = (
        "https://arthurhouhttps.pps.eosdis.nasa.gov/gpmdata/{yyyy}/{mm}/{dd}/"
        "imerg/3B-HHR.MS.MRG.3IMERG.{yyyymmdd}-S{sh}{sm}00-E{eh}{em}59.{MMMM}.V06B.HDF5"
    ).format(
        yyyy=time.strftime("%Y"),
        mm=time.strftime("%m"),
        dd=time.strftime("%d"),
        yyyymmdd=time.strftime("%Y%m%d"),
        sh=time.strftime("%H"),
        sm=time.strftime("%M"),
        eh=time.strftime("%H"),
        em=(time + pd.Timedelta("29 min")).strftime("%M"),
        MMMM=f"{(time.hour*60 + time.minute):04}",
    )
    return input_url_pattern


def _simple_func(*args, **kwargs):
    return None


@pytest.mark.timeout(90)
@pytest.mark.filterwarnings("ignore:Large object")
def test_memory_usage():
    # https://github.com/pangeo-forge/pangeo-forge-recipes/issues/151
    # Requires >4 GiB of memory to run.
    pd = pytest.importorskip("pandas")
    distributed = pytest.importorskip("distributed")

    dates = pd.date_range("2020-05-31T00:00:00", "2021-05-31T23:59:59", freq="30min")
    time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)
    pattern = FilePattern(
        _make_filename_for_memory_usage_test,
        time_concat_dim,
    )

    recipe = XarrayZarrRecipe(
        pattern,
        xarray_open_kwargs={"group": "Grid", "decode_coords": "all"},
        inputs_per_chunk=1,
    )

    delayed = recipe.to_dask()

    # in what follows, we rebuild the dask HighLevelGraph, replacing all of the
    # actual functions with dummy functions that do nothin
    dsk = delayed.dask
    key = delayed.key
    layers = dsk.layers
    dependencies = dsk.dependencies
    newlayers = {}
    for layer_name, subgraph in layers.items():
        new_subgraph = dict(subgraph)
        for k, v in new_subgraph.items():
            if dask.core.istask(v):
                _, *args = v
                new_subgraph[k] = (_simple_func,) + tuple(args)
        newlayers[layer_name] = new_subgraph
    new_hlg = HighLevelGraph(newlayers, dependencies)

    with dask.config.set(
        **{"distributed.worker.memory.pause": 0.95, "distributed.worker.memory.terminate": 0.9}
    ):
        with distributed.Client(n_workers=1, threads_per_worker=1, memory_limit="4G") as client:
            print("submitting")
            client.get(new_hlg, [key])
