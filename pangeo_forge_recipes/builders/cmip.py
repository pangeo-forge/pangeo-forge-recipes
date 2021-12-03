from typing import List, Optional, Union

from cmip6_preprocessing.postprocessing import match_metrics
from cmip6_preprocessing.preprocessing import combined_preprocessing
from cmip6_preprocessing.utils import google_cmip_col

from ..patterns import pattern_from_file_sequence
from ..recipes import XarrayZarrRecipe


def cmip6_weighted_mean_recipe(
    facets: dict,
    weight_coord: str,
    mean_dims: Union[List[str], str],
    coarsen_time: Optional[int] = None,
) -> XarrayZarrRecipe:

    # Initialize CMIP6 catalog
    col = google_cmip_col()

    # Get path to variable data according to facets
    # Note: might be superceded by opener refactor (custom opener which takes facets as kwargs)
    cat = col.search(**facets)
    path_list = cat.df["zstore"].tolist()
    assert len(path_list) == 1
    path = path_list[0]

    # Load all available weights to enable imperfect matching, see:
    # https://cmip6-preprocessing.readthedocs.io/en/latest/postprocessing.html#Handling-grid-metrics-in-CMIP6
    cat_weight = col.search(
        variable_id=weight_coord, source_id=facets["source_id"], grid_label=facets["grid_label"],
    )
    ddict_weight = cat_weight.to_dataset_dict(
        zarr_kwargs={"consolidated": True, "use_cftime": True},
        preprocess=combined_preprocessing,
        aggregate=False,
    )

    def process_input(ds, fname):
        ds = combined_preprocessing(ds)

        ddict = dict(blank=ds)
        ddict = match_metrics(ddict, ddict_weight, [weight_coord])

        assert len(ddict) == 1
        _, ds_out = ddict.popitem()

        # TODO: write weight provenance to ds_out attributes here
        ds_out = ds_out.weighted(ds_out[weight_coord].fillna(0)).mean(mean_dims)

        if coarsen_time is not None:
            ds_out = ds_out.coarsen(time=coarsen_time, boundary="trim").mean()

        return ds_out

    bypass_open = True
    pattern = pattern_from_file_sequence(
        [path], "time", is_opendap=bypass_open,  # Haha, okay this is weird, obviously.
    )
    xarray_open_kwargs = dict(engine="zarr")
    recipe = XarrayZarrRecipe(
        pattern,
        target_chunks={"time": 1},
        xarray_open_kwargs=xarray_open_kwargs,
        process_input=process_input,
    )
    return recipe
