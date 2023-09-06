"""From http://www.climatologylab.org/terraclimate.html:

    TerraClimate is a dataset of monthly climate and climatic water balance for global terrestrial
    surfaces from 1958-2019. These data provide important inputs for ecological and hydrological
    studies at global scales that require high spatial resolution and time-varying data. All data
    have monthly temporal resolution and a ~4-km (1/24th degree) spatial resolution. The data cover
    the period from 1958-2019. We plan to update these data periodically (annually).

This is an advanced example that illustrates the following concepts
- **Multiple variables in different files**: One file per year for a dozen different variables.
- **Complex preprocessing**: We want to apply different preprocessing depending on the variable.
"""
import apache_beam as beam
import xarray as xr

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.transforms import Indexed, OpenURLWithFSSpec, OpenWithXarray, StoreToZarr

# for the example, we only select two years to keep the example small;
# this time range can be extended if you are running the recipe yourself.
years = list(range(2000, 2002))

# even when subsetting to just two years of data, including every variable results
# in a dataset size of rougly 3-3.5 GB. this is a bit large to run for the example.
# to keep the example efficient, we select two of the available variables. to run
# more variables yourself, simply uncomment any/all of the commented variables below.
variables = [
    # "aet",
    # "def",
    # "pet",
    # "ppt",
    # "q",
    "soil",
    "srad",
    # "swe",
    # "tmax",
    # "tmin",
    # "vap",
    # "ws",
    # "vpd",
    # "PDSI",
]


def make_filename(variable, time):
    return (
        "http://thredds.northwestknowledge.net:8080/thredds/fileServer/"
        f"TERRACLIMATE_ALL/data/TerraClimate_{variable}_{time}.nc"
    )


pattern = FilePattern(
    make_filename, ConcatDim(name="time", keys=years), MergeDim(name="variable", keys=variables)
)


class Munge(beam.PTransform):
    """
    Apply cleaning transformations to Datasets
    """

    @staticmethod
    def _apply_mask(key, da):
        """helper function to mask DataArrays based on a threshold value"""
        mask_opts = {
            "PDSI": ("lt", 10),
            "aet": ("lt", 32767),
            "def": ("lt", 32767),
            "pet": ("lt", 32767),
            "ppt": ("lt", 32767),
            "ppt_station_influence": None,
            "q": ("lt", 2147483647),
            "soil": ("lt", 32767),
            "srad": ("lt", 32767),
            "swe": ("lt", 10000),
            "tmax": ("lt", 200),
            "tmax_station_influence": None,
            "tmin": ("lt", 200),
            "tmin_station_influence": None,
            "vap": ("lt", 300),
            "vap_station_influence": None,
            "vpd": ("lt", 300),
            "ws": ("lt", 200),
        }
        if mask_opts.get(key, None):
            op, val = mask_opts[key]
            if op == "lt":
                da = da.where(da < val)
            elif op == "neq":
                da = da.where(da != val)
        return da

    def _preproc(self, item: Indexed[xr.Dataset]) -> Indexed[xr.Dataset]:
        """custom preprocessing function for terraclimate data"""
        import xarray as xr

        index, ds = item

        # invalid unicode in source data. This attr replacement is a fix.
        # FIXME: use lighter solution from:
        # https://github.com/pangeo-forge/pangeo-forge-recipes/issues/586
        fixed_attrs = {
            "method": (
                "These layers from TerraClimate were derived from the essential climate variables "
                "of TerraClimate. Water balance variables, actual evapotranspiration, climatic "
                "water deficit, runoff, soil moisture, and snow water equivalent were calculated "
                "using a water balance model and plant extractable soil water capacity derived "
                "from Wang-Erlandsson et al (2016)."
            ),
            "title": (
                "TerraClimate: monthly climate and climatic water balance for global land surfaces"
            ),
            "summary": (
                "This archive contains a dataset of high-spatial resolution (1/24th degree, ~4-km) "
                "monthly climate and climatic water balance for global terrestrial surfaces from "
                "1958-2015. These data were created by using climatically aided interpolation, "
                "combining high-spatial resolution climatological normals from the WorldClim "
                "version 1.4 and version 2 datasets, with coarser resolution time varying "
                "(i.e. monthly) data from CRU Ts4.0 and JRA-55 to produce a monthly dataset of "
                "precipitation, maximum and minimum temperature, wind speed, vapor pressure, and "
                "solar radiation. TerraClimate additionally produces monthly surface water balance "
                "datasets using a water balance model that incorporates reference "
                "evapotranspiration, precipitation, temperature, and interpolated plant "
                "extractable soil water capacity."
            ),
            "keywords": (
                "WORLDCLIM,global,monthly, temperature,precipitation,wind,radiation,vapor "
                "pressure, evapotranspiration,water balance,soil water capacity,snow water "
                "equivalent,runoff"
            ),
            "id": "Blank",
            "naming_authority": "edu.uidaho.nkn",
            "keywords_vocabulary": "None",
            "cdm_data_type": "GRID",
            "history": "Created by John Abatzoglou, University of California Merced",
            "date_created": "2021-04-22",
            "creator_name": "John Abatzoglou",
            "creator_url": "http://climate.nkn.uidaho.edu/TerraClimate",
            "creator_role": "Principal Investigator",
            "creator_email": "jabatzoglou@ucmerced.edu",
            "institution": "University of California Merced",
            "project": "Global Dataset of Monthly Climate and Climatic Water Balance (1958-2015)",
            "processing_level": "Gridded Climate Projections",
            "acknowledgment": (
                "Please cite the references included herein. We also acknowledge the WorldClim "
                "datasets (Fick and Hijmans, 2017; Hijmans et al., 2005) and the CRU Ts4.0 "
                "(Harris et al., 2014) and JRA-55 (Kobayashi et al., 2015) datasets."
            ),
            "geospatial_lat_min": -89.979164,
            "geospatial_lat_max": 89.979164,
            "geospatial_lon_min": -179.97917,
            "geospatial_lon_max": 179.97917,
            "geospatial_vertical_min": 0.0,
            "geospatial_vertical_max": 0.0,
            "time_coverage_start": "1958-01-01T00:0",
            "time_coverage_end": "1958-12-01T00:0",
            "time_coverage_duration": "P1Y",
            "time_coverage_resolution": "P1M",
            "standard_nam_vocabulary": "CF-1.0",
            "license": "No restrictions",
            "contributor_name": "Katherine Hegewisch",
            "contributor_role": "Postdoctoral Fellow",
            "contributor_email": "khegewisch@ucmerced.edu",
            "publisher_name": "Northwest Knowledge Network",
            "publisher_url": "http://www.northwestknowledge.net",
            "publisher_email": "info@northwestknowledge.net",
            "date_modified": "2021-04-22",
            "date_issued": "2021-04-22",
            "geospatial_lat_units": "decimal degrees north",
            "geospatial_lat_resolution": -0.041666668,
            "geospatial_lon_units": "decimal degrees east",
            "geospatial_lon_resolution": 0.041666668,
            "geospatial_vertical_units": "None",
            "geospatial_vertical_resolution": 0.0,
            "geospatial_vertical_positive": "Up",
            "references": (
                "Abatzoglou, J.T., S.Z. Dobrowski, S.A. Parks, and K.C. Hegewisch, 2017, "
                "High-resolution global dataset of monthly climate and climatic water "
                "balance from 1958-2015, submitted to Scientific Data."
            ),
            "source": "WorldClim v2.0 (2.5m), CRU Ts4.0, JRA-55",
            "version": "v1.0",
            "Conventions": "CF-1.6",
        }
        ds.attrs = fixed_attrs

        rename = {}

        station_influence = ds.get("station_influence", None)

        if station_influence is not None:
            ds = ds.drop_vars("station_influence")

        var = list(ds.data_vars)[0]

        rename_vars = {"PDSI": "pdsi"}

        if var in rename_vars:
            rename[var] = rename_vars[var]

        if "day" in ds.coords:
            rename["day"] = "time"

        if station_influence is not None:
            ds[f"{var}_station_influence"] = station_influence
        with xr.set_options(keep_attrs=True):
            ds[var] = self._apply_mask(var, ds[var])
        if rename:
            ds = ds.rename(rename)
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._preproc)


recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern.file_type)
    | Munge()  # Custom pre-processor
    | StoreToZarr(
        store_name="terraclimate.zarr",
        combine_dims=pattern.combine_dim_keys,
        target_chunks={"lat": 1024, "lon": 1024, "time": 12},
    )
)
