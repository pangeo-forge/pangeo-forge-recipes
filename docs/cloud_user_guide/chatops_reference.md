# ChatOps Reference

Once you've opened a Pull Request (PR) with your [Recipe Contribution](./recipe_contribution.md), a series of
automated checks is performed to ensure that the submitted files conform to the expected format.

If any changes are necessary, [`@pangeo-forge-bot`](https://github.com/pangeo-forge-bot) will report back with a comment.

## Overview

The checks on this page are listed sequentially according to the order in which they are performed.

All checks up to and including [Recipe run(s) created](chatops_reference.md#recipe-runs-created) are automatically run against the latest commit of your PR _**each time you push**_ to the PR branch.

If any required changes are identified by [`@pangeo-forge-bot`](https://github.com/pangeo-forge-bot),
all checks will be re-run as soon as you push new commit(s) to the PR branch.

```{graphviz}

digraph chatops {
    newrank=true;
    graph [splines=ortho];

    subgraph cluster_pr_structure {
        color = "#003B71";
        shape=rect;
        style=rounded;

        label="PR Structure";
        href="./chatops_reference.html#pr-structure";

        node [shape=rect, style=rounded, color="#5eb130"];
        a [label = "All changes in recipes/ subdir"];
        b [label = "Single layer of subdirectories"];
        c [label = "Only one subdirectory"];
    }

    subgraph cluster_meta_yaml {
        color = "#003B71";
        shape=rect;
        style=rounded;

        label="meta.yaml";
        labelloc="b";
        href="./chatops_reference.html#meta-yaml";

        node [shape=rect, style=rounded, color="#5eb130"];
        d [label = "Presence"];
        e [label = "Loadability"];
        f [label = "Completeness"];
    }

    subgraph cluster_recipe_module {
        color = "#003B71";
        shape=rect;
        style=rounded;

        label="Recipe module";
        href="./chatops_reference.html#recipe-module";

        node [shape=rect, style=rounded, color="#5eb130"];
        g [label = "Presence"];
        h [label = "Recipe run(s) created"];
        i [label = "/run recipe-test"];
        j [label = "Importability"];
        k [label = "Test status: in_progress"];
        l [label = "Test status: failed"];
        m [label = "Test status: success"];
    }

    {rank = same; a; g;}
    {rank = same; b; h;}
    {rank = same; c; i;}
    {rank = same; d; j;}
    {rank = same; e; k;}

    a -> b -> c -> d -> e -> f;
    f:e -> g:w;
    g -> h -> i -> j -> k;
    k:s -> l;
    k:s -> m;


}

```

The following is a listing of the automated checks performed on your PR, along with examples of the comments that
[`@pangeo-forge-bot`](https://github.com/pangeo-forge-bot) makes in response to various check results.

## PR Structure

As described in [Recipe Contribution](./recipe_contribution.md) and [Introduction Tutorial Part 3](../introduction_tutorial/intro_tutorial_part3.ipynb#add-the-recipe-files), your PR to the [`pangeo-forge/staged-recipes`](https://github.com/pangeo-forge/staged-recipes) repository should add a single new directory within the `recipes/` subdirectory:

```
staged-recipes/recipes/
                └──{dataset-name}/
                        ├──meta.yaml
                        └──{recipe-module-name}.py
```

The first check run against all PRs is that the content of the PR adheres to this structure.

### All changes in `recipes/` subdir

If your PR has changed files outside of the `recipes/` subdirectory, you will receive a comment notification like this:

````{panels}
:column: col-lg-12 p-2

{{ pangeo_forge_bot_header }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It looks like there may be a problem with the structure of your PR.

I encountered a `FilesChangedOutsideRecipesSubdirError("This PR changes files outside the ``recipes/`` directory.")`.
````

Moving all changes within the `recipes/` subdirectory will resolve this error.

### Single layer of subdirectories

If your PR contains additional subdirectories within the `recipes/{dataset-name}/` directory, you will receive a comment notification like this:

````{panels}
:column: col-lg-12 p-2

{{ pangeo_forge_bot_header }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It looks like there may be a problem with the structure of your PR.

I encountered a `MultipleLayersOfSubdirectoriesError('This PR uses more than one layer of subdirs.')`.
````

Placing all submitted files directly within the `recipes/{dataset-name}/` directory will resolve this error.

### Only one subdirectory

If your PR contains more than one subdirectory within the `recipes/` directory, (e.g., `recipes/dataset-name-0/` _and_ `recipes/dataset-name-1/`, etc.) you will receive a comment notification like this:

````{panels}
:column: col-lg-12 p-2

{{ pangeo_forge_bot_header }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It looks like there may be a problem with the structure of your PR.

I encountered a `TooManySubdirectoriesError("Not all files in this PR exist within the same subdirectory of ``recipes/``.")`.
````

Removing all but one subdirectory of `recipes/` from your PR will resolve this error.

## `meta.yaml`

Once the content of your PR is found to [adhere to the expected structure](./chatops_reference.md#pr-structure), the next aspect that is checked is the `meta.yaml` file.

```{note}
In what follows, we will assume that your dataset name is `woa-1deg-monthly` and therefore your submitted files should all exist within the directory `recipes/woa-1deg-monthly/`.
```

### Presence

The first `meta.yaml` check is simply to confirm that a file named _**exactly**_ `meta.yaml` exists within your PR subdirectory. If no such file is found, you will recieve a comment notification such as:

````{panels}
:column: col-lg-12 p-2

{{ pangeo_forge_bot_header }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

I don't see a `meta.yaml` in this PR, only these files:
```python
['recipes/woa-1deg-monthly/meta.yml', 'recipes/woa-1deg-monthly/recipe.py']
```
Please commit a `meta.yaml` that follows this [template](https://github.com/pangeo-forge/sandbox/blob/main/recipe/meta.yaml).
> _**Sorry, I only recognize the longform `.yaml` extension!**_
> _If you're using the shortform `.yml`, please update your filename to use the longform extension._

````

Note that the error may arise from the fact that file is truly missing, or perhaps just that its name is not _**exactly**_ `meta.yaml`. In the example above, changing the filename as follows

```diff
- meta.yml
+ meta.yaml
```
will resolve the error.

### Loadability

Pangeo Forge Cloud uses [PyYAML](https://pyyaml.org/wiki/PyYAML)'s `yaml.safe_load` to load the `meta.yaml`. If your `meta.yaml` cannot be loaded with this function, you may receive a comment notification such as:

````{panels}
:column: col-lg-12 p-2

{{ pangeo_forge_bot_header }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When I tried to load `'recipes/woa-1deg-monthly/meta.yaml'`, I got a `ScannerError`.
You should be able to replicate this error yourself.

First make sure you're in the root of your cloned `staged-recipes` repo. Then run this code in a Python interpreter:
```python
import yaml  # note: `pip install PyYAML` first

with open("recipes/woa-1deg-monthly/meta.yaml", "r") as f:
    yaml.safe_load(f)
```
Please correct `meta.yaml` so that you're able to run this code without error, then commit the corrected `meta.yaml`.
````

This notification will only arise if your `meta.yaml` is not a properly
formatted YAML file. Following the instructions in the comment will allow
you to replicate the error, which is often caused by small mistakes such as incorrect indentation or missing/incorrect punctuation (i.e. misplaced `-` dashes or `:` colons). Commiting a corrected `meta.yaml` which can be loaded with `yaml.safe_load` without error will allow you to move past this check.


### Completeness

Once your `meta.yaml` can be loaded, the completeness check confirms that all
expected fields are included in the file. If any fields are found to be missing, you will receive a comment notification such as:

````{panels}
:column: col-lg-12 p-2

{{ pangeo_forge_bot_header }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It looks like your `meta.yaml` does not conform to the specification.

```python-traceback
            2 validation errors for MetaYaml
pangeo_notebook_version
  field required (type=value_error.missing)
maintainers -> 0 -> orcid
  field required (type=value_error.missing)

```

Please correct your `meta.yaml` and commit the corrections to this PR.

````

In this example, the `meta.yaml` was found to be missing the `pangeo_notebook_version` field and the `orchid` ID for one of the recipe maintainers. Adding the missing fields will resolve this error.

For a complete reference of required fields for the `meta.yaml`, see [Required files](./recipe_contribution.md#required-files).

## Recipe module

Once the `meta.yaml` is found to be [present, loadable, and complete](./chatops_reference.md#metayaml), checks of the recipe module begin.

### Presence

The first check is for the presence of the recipe module.

Pangeo Forge Cloud does not require any specific name for the recipe module. Instead, as described in the [Required files](recipe_contribution.md#required-files) section, the name of the recipe module is defined in the `meta.yaml`.

If a recipe module with the name indicated in `meta.yaml` is not found in the PR, you will receive a comment notification such as:

````{panels}
:column: col-lg-12 p-2

{{ pangeo_forge_bot_header }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

I'm having trouble finding your recipe module (i.e. Python file) in this PR.

Your `meta.yaml` recipes section currently includes a recipe declared as:
```yaml
- id: woa18
  object: recipe:woa18_recipe
```
The `object` here should conform to the format `{recipe-module-name}:{recipe-object-name}`.

In your PR I only see the following files:
```python
['recipes/woa-1deg-monthly/meta.yaml', 'recipes/woa-1deg-monthly/recipy.py']
```
...none of which end with `/recipe.py`, which is unexpected given the `object` shown above.

Please help me find your recipe module by either:
- Updating the `meta.yaml` recipes section `object` declaration to point to an existing module name; or
- Changing the names of the `.py` files in this PR to point to the existing `object` in your `meta.yaml`

````

This error may occur due to the recipe module truly being missing, or perhaps due to an inconsistency between the recipe module name indicated in `meta.yaml` and the name of the actual file in the PR. In the case of the example above, a simple typo is causing the error. Changing the recipe module name in the PR as follows:

```diff
- recipy.py
+ recipe.py
```
will resolve the error.

### Recipe run(s) created

```{note}
The checks on this page are listed sequentially according to the order in which they are performed.

All checks up to and including [Recipe run(s) created](chatops_reference.md#recipe-runs-created) are automatically run against the latest commit of your PR _**each time you push**_ to the PR branch.
```

Once the recipe module's presence is confirmed, a new [Recipe Run](./core_concepts.md#recipe-runs) is registered with Pangeo Forge Cloud for every recipe included in the PR. When this is complete, you will receive a comment notification such as:

````{panels}
:column: col-lg-12 p-2

{{ pangeo_forge_bot_header }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

🎉 New recipe runs created for the following recipes at sha `6a1e73e`:
 - `woa18`: <https://pangeo-forge.org/dashboard/recipe-run/>`{recipe_run_id}`

````

where `{recipe_run_id}` will be replaced with an integer value corresponding to this particular recipe run. If your PR defines more than one recipe, the comment notification will include additional bullet points, one for each recipe in the PR.

The link in the above example comment does not resolve to a real webpage, because it does not have a `{recipe_run_id}` assigned to it. Please refer to

> <https://pangeo-forge.org/dashboard/recipe-runs/>

for a listing of real [Recipe Runs](./core_concepts.md#recipe-runs).

### `/run recipe-test`

Automatically created recipe runs all start with a status of `queued`. To move
the status of a recipe run to `in_progress` (thereby beginning the actual test
execution of the recipe), a human maintainer of Pangeo Forge must issue a
special chatops command, as follows:

````{panels}
:column: col-lg-12 p-2

{{ human_maintainer_header }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

/run recipe-test recipe_run_id=`{recipe_run_id}`

````
in this example, `{recipe_run_id}` would be replaced with the integer id
number of the recipe run to be run.

### Importability

The first thing that happens following a Pangeo Forge maintainer issuing the `/run recipe-test` command is a check that the recipe module is importable. If
the recipe module calls local variables or packages which have not been assigned and/or imported, a `NameError` will occur on import, and you will
receive a comment notification such as:

````{panels}
:column: col-lg-12 p-2

{{ pangeo_forge_bot_header }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When I tried to import your recipe module, I encountered this error

```python-traceback
line 43, in <module>
    pattern = patterns.FilePattern(format_function, variable_merge_dim, month_concat_dim)
NameError: name 'format_function' is not defined
```

Please correct your recipe module so that it's importable.
````



### Test status: `in_progress`

````{panels}
:column: col-lg-12 p-2

{{ pangeo_forge_bot_header }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

✨ A test of your recipe `woa18` is now running on Pangeo Forge Cloud!

I'll notify you with a comment on this thread when this test is complete. (This could be a little while...)

In the meantime, you can follow the logs for this recipe run at <https://pangeo-forge.org/dashboard/recipe-run/>`{recipe_run_id}`
````

### Test status: `failed`

````{panels}
:column: col-lg-12 p-2

{{ pangeo_forge_bot_header }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Pangeo Forge Cloud told me that our test of your recipe `woa18-1deg-monthly` failed. But don't worry, I'm sure we can fix this!

To see what error caused the failure, please review the logs at <https://pangeo-forge.org/dashboard/recipe-run/10>

If you haven't yet tried [pruning and running your recipe locally](../introduction_tutorial/intro_tutorial_part2.ipynb#prune-the-recipe), I suggest trying that now.

Please report back on the results of your local testing in a new comment below, and a Pangeo Forge maintainer will help you with next steps!
````


### Test status: `success`

````{panels}
:column: col-lg-12 p-2

{{ pangeo_forge_bot_header }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

🥳 **Hooray!**  The test execution of your recipe `woa18-1deg-monthly` succeeded.

Here is a static representation of the dataset built by this recipe:

<details>

```
            <xarray.Dataset>
Dimensions:             (time: 2, depth: 57, lat: 180, lon: 360, nbounds: 2)
Coordinates:
    climatology_bounds  (time, nbounds) float32 dask.array<chunksize=(1, 2), meta=np.ndarray>
    crs                 int32 ...
  * depth               (depth) float32 0.0 5.0 10.0 ... 1.45e+03 1.5e+03
    depth_bnds          (depth, nbounds) float32 dask.array<chunksize=(57, 2), meta=np.ndarray>
  * lat                 (lat) float32 -89.5 -88.5 -87.5 -86.5 ... 87.5 88.5 89.5
    lat_bnds            (lat, nbounds) float32 dask.array<chunksize=(180, 2), meta=np.ndarray>
  * lon                 (lon) float32 -179.5 -178.5 -177.5 ... 177.5 178.5 179.5
    lon_bnds            (lon, nbounds) float32 dask.array<chunksize=(360, 2), meta=np.ndarray>
  * time                (time) object 1986-01-16 00:00:00 1958-02-16 00:00:00
Dimensions without coordinates: nbounds
Data variables: (12/40)
    A_an                (time, depth, lat, lon) float32 dask.array<chunksize=(1, 57, 180, 360), meta=np.ndarray>
    A_dd                (time, depth, lat, lon) float64 dask.array<chunksize=(1, 57, 180, 360), meta=np.ndarray>
    A_gp                (time, depth, lat, lon) float64 dask.array<chunksize=(1, 57, 180, 360), meta=np.ndarray>
    A_ma                (time, depth, lat, lon) float32 dask.array<chunksize=(1, 57, 180, 360), meta=np.ndarray>
    A_mn                (time, depth, lat, lon) float32 dask.array<chunksize=(1, 57, 180, 360), meta=np.ndarray>
    A_oa                (time, depth, lat, lon) float32 dask.array<chunksize=(1, 57, 180, 360), meta=np.ndarray>
    ...                  ...
    t_gp                (time, depth, lat, lon) float64 dask.array<chunksize=(1, 57, 180, 360), meta=np.ndarray>
    t_ma                (time, depth, lat, lon) float32 dask.array<chunksize=(1, 57, 180, 360), meta=np.ndarray>
    t_mn                (time, depth, lat, lon) float32 dask.array<chunksize=(1, 57, 180, 360), meta=np.ndarray>
    t_oa                (time, depth, lat, lon) float32 dask.array<chunksize=(1, 57, 180, 360), meta=np.ndarray>
    t_sd                (time, depth, lat, lon) float32 dask.array<chunksize=(1, 57, 180, 360), meta=np.ndarray>
    t_se                (time, depth, lat, lon) float32 dask.array<chunksize=(1, 57, 180, 360), meta=np.ndarray>
Attributes: (12/49)
    Conventions:                     CF-1.6, ACDD-1.3
    cdm_data_type:                   Grid
    comment:                         global climatology as part of the World ...
    contributor_name:                Ocean Climate Laboratory
    contributor_role:                Calculation of climatologies
    creator_email:                   NCEI.info@noaa.gov
    ...                              ...
    summary:                         Climatological mean Apparent Oxygen Util...
    time_coverage_duration:          P!!Y
    time_coverage_end:               2017-01-31
    time_coverage_resolution:        P01M
    time_coverage_start:             1900-01-01
    title:                           World Ocean Atlas 2018 : Apparent_Oxygen...
```

</details>

You can also open this dataset by running the following Python code

```python
import fsspec
import xarray as xr

dataset_public_url = 'https://ncsa.osn.xsede.org/Pangeo/pangeo-forge-test/prod/recipe-run-11/pangeo-forge/staged-recipes/woa18-1deg-monthly.zarr'
mapper = fsspec.get_mapper(dataset_public_url)
ds = xr.open_zarr(mapper, consolidated=True)
ds
```

in this [![badge](https://img.shields.io/badge/scratch-%20notebook-579ACA.svg?logo=data:image/png;base64,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)](https://mybinder.org/v2/gh/pangeo-forge/sandbox/binder?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fpangeo-forge%252Fsandbox%26urlpath%3Dlab%252Ftree%252Fsandbox%252Fscratch.ipynb%26branch%3Dmain) (or your Python interpreter of choice).

<h3> Checklist </h3>
Please copy-and-paste the list below into a new comment on this thread, and check the boxes off as you've reviewed them.

> **Note**: This test execution is limited to two increments in the concatenation dimension, so you should expect the length of that dimension (e.g, `"time"` or equivalent) to be `2`.

```
- [ ] Are the dimension lengths correct?
- [ ] Are all of the expected variables present?
- [ ] Does plotting the data produce a plot that looks like your dataset?
- [ ] Can you run a simple computation/reduction on the data and produce a plausible result?
```

````


````{panels}
:column: col-lg-12 p-2

{{ recipe_contributor_header }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- ☑️ Are the dimension lengths correct?
- ☑️ Are all of the expected variables present?
- ☑️ Does plotting the data produce a plot that looks like your dataset?
- ☑️ Can you run a simple computation/reduction on the data and produce a plausible result?

````
