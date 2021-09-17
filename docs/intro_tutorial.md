# Introduction Tutorial

Welcome to Pangeo Forge. This tutorial will guide you through producing your first analysis-ready, cloud-optimized (ARCO) dataset.

To create your ARCO dataset, you'll need to create a new recipe.

### Overview

1. [fork](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/fork-a-repo) the <https://github.com/pangeo-forge/staged-recipes> GitHub repository
2. Develop the recipe (see below)
3. Submit the new recipe as a pull request

At this point the pangeo-forge maintainers (and [a host of bots](https://github.com/pangeo-bot)) will verify that your recipe is shipshape and ready for inclusion in pangeo-forge.
See [](#maintaining) for more on what happens next.

![Architecture: High Level View](https://github.com/pangeo-forge/flow-charts/blob/main/renders/architecture.png?raw=true)


See [maintaining](##maintaining) for more on what happens next.

## Developing a Recipe

### Ingredients

Each recipe requies two primary files: `meta.yaml` and `recipe.py`. Each are described below:

#### `meta.yaml`

The `meta.yaml` file includes the metadata that describes your recipe. A full example is availble in the [staged-recipes repository](https://github.com/conda-forge/staged-recipes/blob/main/recipes/example/meta.yaml). Below some of the key sections are highlighted

specify recipe environment:

```yaml
pangeo_forge_version: "0.5.0"  # pypi version of pangeo-forge
pangeo_notebook_version: "2021.07.17"  # docker image that the flow will run in
```

Define the recipe name and where to find the recipe object:

```yaml
recipes:
  - id: noaa-oisst-avhrr-only  # name of feedstock?
    object: "recipe:recipe"  # import schema for recipe object `{module}:{object}
```


Specify bakery and compute resources:

```yaml
bakery:
  id: "devseed.bakery.development.aws.us-west-2"  # must come from a valid list of bakeries -- Where is this? Link to valid list of bakeries
  target: pangeo-forge-aws-bakery-flowcachebucketdasktest4-10neo67y7a924
  resources:
    memory: 4096
    cpu: 1024
```

The final section includes metadata about the dataset and maintainers of the recipe. This dataset metadata ?can/will? be used to create a STAC catalog to aid in dataset discoverability.

```yaml
title: "NOAA Optimum Interpolated SST" #Dataset title
description: "Analysis-ready Zarr datasets derived from NOAA OISST NetCDF" #Short dataset description
provenance:
  providers:
    - name: "NOAA NCEI" #dataset distributor/source
      description: "National Oceanographic & Atmospheric Administration National Centers for Environmental Information"
      roles:
        - producer
        - licensor
      url: https://www.ncdc.noaa.gov/oisst
  license: "CC-BY-4.0"
maintainers:
  - name: "Ryan Abernathey"
    orcid: "0000-0001-5999-4917"
    github: rabernat
```

#### `recipe.py`

The `recipe.py` file is where the processing steps are defined.  For detailed descriptions on recipe ingredients, check out the [recipe-user-guide](https://pangeo-forge.readthedocs.io/en/latest/recipe_user_guide/index.html#)

Multiple recipe examples dealing more more complex data cleaning/processing can be found on the [tutorials page](https://pangeo-forge.readthedocs.io/en/latest/tutorials/index.html)

Imports:

```python
import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
```

Define inputs to the recipe:

```python
start_date = "1981-09-01"
end_date = "2021-01-05"


def format_function(time):
    base = pd.Timestamp(start_date)
    day = base + pd.Timedelta(days=time)
    input_url_pattern = (
        "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation"
        "/v2.1/access/avhrr/{day:%Y%m}/oisst-avhrr-v02r01.{day:%Y%m%d}.nc"
    )
    return input_url_pattern.format(day=day)


dates = pd.date_range(start_date, end_date, freq="D")
```
The `FilePattern` part of the recipe is a crucial bit that defines the location of the input files. By exploring the input data source, you can usually determine a basic filepattern and then recreate it in the `FilePattern` part of the recipe.


More details and examples on can be found in the [File Patterns Explainer](https://pangeo-forge.readthedocs.io/en/latest/recipe_user_guide/file_patterns.html)


```python
pattern = FilePattern(format_function, ConcatDim("time", range(len(dates)), 1))
```

Construct the recipe:

```python
recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=20, cache_inputs=True)
```


### Testing

#### Local Testing

With our previous recipe construction, we can create a pruned copy of the first two entries for testing.
```python
recipe = recipe.copy_pruned()
```

Using ffspec and pangeo_forge_recipes, we can create a `LocalFileSystem` to cache recipe data. If you wish you can use any ffspec file system instead of a `LocalFileSystem` ex. (s3fs, gcsfs etc.)

```python
from fsspec.implementations.local import LocalFileSystem
from pangeo_forge_recipes.storage import MetadataTarget, CacheFSSpecTarget

fs_local = LocalFileSystem()

recipe.input_cache = CacheFSSpecTarget(fs_local, "<filepath_for_input_cache>")
recipe.metadata_cache = MetadataTarget(fs_local, "<filepath_for_metadata>")
recipe.target = MetadataTarget(fs_local, "<filepath_for_zarr_store>")
```
Optionally we can setup logging to see under the hood a bit.

```python
def setup_logging():
    import logging
    import sys
    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("pangeo_forge_recipes")
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
setup_logging()
```
Next we can test run our pruned recipe using Prefect.

```python
flow = recipe.to_prefect()
flow.run()
```

Finally we can verify a slice of the dataset

```python
ds_target = xr.open_zarr(recipe.target.get_mapper(), consolidated=True)
ds_target
```

### Submitting the Recipe

Once the local recipe testing
passes successfully, you can submit the recipe for execution.
To do this, create a pull request in the [staged-recipes repository](https://github.com/pangeo-forge/staged-recipes)

#### Automated Tests

Once a pull request of the recipe has been submitted, one of the pangeo-forge maintainers can trigger a CI test by running:

```
/run-recipe-test
```

?How does a submitter know when their data is sent to a bakery/processed?

## Data Access/Catalog
How is the data accessed once finished?


## Maintaining a Recipe

What do we want to include here:
