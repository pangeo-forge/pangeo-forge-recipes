# Release Notes

## v0.9.4 - 2023-01-06

- Fixed bug in `0.9.3` which broke all recipe object imports if `scipy` (an optional
dependency) was missing from environment. {pull}`466`

## v0.9.3 - 2023-01-03

```{warning}
Yanked due to presence of a bug. Please use `0.9.4` instead.
```

- Bugfix to allow opening of zarr files. This fix allows using Zarr stores as an input
source for recipes. {pull}`462`
- Add netcdf3 support for opening source files with kerchunk. Resolves a long-standing
issue wherein netcdf3 source files could not be loaded lazily, which effectively
blocked the use of large netcdf3 files as recipe sources. {pull}`383`
- Fix zarr reference bug {pull}`455`
- Add `dataset_type` class attribute for recipe classes {pull}`437`


## v0.9.2 - 2022-10-26

- Bump peter-evans/slash-command-dispatch from 2 to 3 [#434](https://github.com/pangeo-forge/pangeo-forge-recipes/pull/434) ([@dependabot](https://github.com/dependabot))
- Bump actions/setup-python from 3 to 4 [#433](https://github.com/pangeo-forge/pangeo-forge-recipes/pull/433) ([@dependabot](https://github.com/dependabot))
- Bump LouisBrunner/checks-action from 1.2.0 to 1.5.0 [#432](https://github.com/pangeo-forge/pangeo-forge-recipes/pull/432) ([@dependabot](https://github.com/dependabot))
- Bump codecov/codecov-action from 2.0.2 to 3.1.1 [#431](https://github.com/pangeo-forge/pangeo-forge-recipes/pull/431) ([@dependabot](https://github.com/dependabot))
- Fix CI [#430](https://github.com/pangeo-forge/pangeo-forge-recipes/pull/430) ([@andersy005](https://github.com/andersy005))
- Avoid serialization blues by computing + caching the hash. [#429](https://github.com/pangeo-forge/pangeo-forge-recipes/pull/429) ([@alxmrs](https://github.com/alxmrs))
- [pre-commit.ci] pre-commit autoupdate [#426](https://github.com/pangeo-forge/pangeo-forge-recipes/pull/426) ([@pre-commit-ci](https://github.com/pre-commit-ci))
- Cancel Redundant CI jobs [#424](https://github.com/pangeo-forge/pangeo-forge-recipes/pull/424) ([@andersy005](https://github.com/andersy005))
- Remove pre-commit action in favor of pre-commit.ci [#423](https://github.com/pangeo-forge/pangeo-forge-recipes/pull/423) ([@andersy005](https://github.com/andersy005))
- Add SciPy video and slides to what_is_pangeo_forge.md [#416](https://github.com/pangeo-forge/pangeo-forge-recipes/pull/416) ([@rabernat](https://github.com/rabernat))
- Updated release_notes.md in preparation for a tag & release. [#413](https://github.com/pangeo-forge/pangeo-forge-recipes/pull/413) ([@alxmrs](https://github.com/alxmrs))
- Document how to access data with Globus [#408](https://github.com/pangeo-forge/pangeo-forge-recipes/pull/408) ([@rabernat](https://github.com/rabernat))
- [pre-commit.ci] pre-commit autoupdate [#381](https://github.com/pangeo-forge/pangeo-forge-recipes/pull/381) ([@pre-commit-ci](https://github.com/pre-commit-ci))

### Contributors to this release

([GitHub contributors page for this release](https://github.com/pangeo-forge/pangeo-forge-recipes/graphs/contributors?from=2022-09-09&to=2022-10-26&type=c))

[@alxmrs](https://github.com/search?q=repo%3Apangeo-forge%2Fpangeo-forge-recipes+involves%3Aalxmrs+updated%3A2022-09-09..2022-10-26&type=Issues) | [@andersy005](https://github.com/search?q=repo%3Apangeo-forge%2Fpangeo-forge-recipes+involves%3Aandersy005+updated%3A2022-09-09..2022-10-26&type=Issues) | [@dependabot](https://github.com/search?q=repo%3Apangeo-forge%2Fpangeo-forge-recipes+involves%3Adependabot+updated%3A2022-09-09..2022-10-26&type=Issues) | [@derekocallaghan](https://github.com/search?q=repo%3Apangeo-forge%2Fpangeo-forge-recipes+involves%3Aderekocallaghan+updated%3A2022-09-09..2022-10-26&type=Issues) | [@martindurant](https://github.com/search?q=repo%3Apangeo-forge%2Fpangeo-forge-recipes+involves%3Amartindurant+updated%3A2022-09-09..2022-10-26&type=Issues) | [@pre-commit-ci](https://github.com/search?q=repo%3Apangeo-forge%2Fpangeo-forge-recipes+involves%3Apre-commit-ci+updated%3A2022-09-09..2022-10-26&type=Issues) | [@rabernat](https://github.com/search?q=repo%3Apangeo-forge%2Fpangeo-forge-recipes+involves%3Arabernat+updated%3A2022-09-09..2022-10-26&type=Issues) | [@yuvipanda](https://github.com/search?q=repo%3Apangeo-forge%2Fpangeo-forge-recipes+involves%3Ayuvipanda+updated%3A2022-09-09..2022-10-26&type=Issues)


## v0.9.1 - 2022-09-08

- Persist Pangeo Forge execution context metadata in target datasets. This information, which includes
the `pangeo-forge-recipes` version as well as recipe and input hashes, attaches execution provenance
to the dataset itself. {pull}`359`
- File pattern support for `.tiff`s. {pull}`393`
- Improved `HDFReferenceRecipe` by passing `target_options` to the `MultiZarrToZarr` class.
- Typo fixes, documentation updates, and project health improvements. {pull}`364`, {pull}`365`, {pull}`366`,
  {pull}`388`, {pull}`396`, {pull}`394`, {pull}`398`, {pull}`407`.
- Fixed `XarrayZarrRecipe`'s `finalize_target()` stage by using bulk delete APIs for consolidating Zarr coordinates.

## v0.9 - 2022-05-11

- **Breaking changes:** Deprecated `XarrayZarrRecipe` manual stage methods. Manual execution can be
performed with any of the executors described in {doc}`../recipe_user_guide/execution`. Also deprecated
`FilePattern(..., is_opendap=True)` kwarg, which is superseded by `FilePattern(..., file_type="opendap")`. {pull}`362`
- Added `serialization` module along with `BaseRecipe.sha256` and `FilePattern.sha256` methods.
Collectively, this provides for generation of deterministic hashes for both recipe and file
pattern instances. Checking these hashes against those from a prior version of the recipe can be
used to determine whether or not a particular recipe instance in a Python module (which may
contain arbitrary numbers of recipe instances) has changed since the last time the instances in
that module were executed. The file pattern hashes are based on a blockchain built cumulatively
from all of the index:filepath pairs yielded by the pattern's `self.items()` method. As such, in
cases where a new pattern is intended to append to an existing dataset which was built from a
prior version of that pattern, the pattern hash can be used to determine the index from which to
begin appending. This is demonstrated in the tests. {pull}`349`
- Created new Prefect executor which wraps the Dask executor in a single Task.
This should mitigate problems related to large numbers of Prefect Tasks ({issue}`347`).
See {doc}`../recipe_user_guide/execution` for details.
- Implemented feature to cap cached filename lengths at 255 bytes on local filesystems, to
accomodate the POSIX filename length limit. Cached filename lengths are not truncated on any other
filesystem. {pull}`353`

## v0.8.3 - 2022-04-19

- Added `.file_type` attribute to {class}`pangeo_forge_recipes.patterns.FilePattern`. This attribute will eventually supercede
`.is_opendap`, which will be deprecated in `0.9.0`. Until then, `FilePattern(..., is_opendap=True)` is supported as equivalent
to `FilePattern(..., file_type="opendap")`. {pull}`322`

## v0.8.2 - 2022-02-23

- Removed click from dependencies and removed cli entrypoint.

## v0.8.1 - 2022-02-23

- Fixed dependency issue with pip installation.
- Fixed bug where recipes would fail if the target chunks exceeded the full
  array length. {issue}`279`

## v0.8.0 - 2022-02-17

- **Breaking change:** Replace recipe classes' storage attibutes with `.storage_config` of type {class}`pangeo_forge_recipes.storage.StorageConfig`. {pull}`288`
- Add `setup_logging` convenience function. {pull}`287`

## v0.7.0 - 2022-02-14 ❤️

- Apache Beam executor added. {issue}`169`. By [Alex Merose](https://github.com/alxmrs).
- Dask executor updates. {pull}`260` {pull}`261`
- Index type update. {pull}`257`
- Fix incompatibility with `fsspec>=2021.11.1`. {pull}`247`

## v0.6.1 - 2021-10-25

- Major internal refactor of executors. {pull}`219`.
  Began deprecation cycle for recipe methods (e.g. `recipe.prepare_target()`) in
  favor of module functions.
- Addition of `open_input_with_fsspec_reference` option on {class}`pangeo_forge_recipes.recipes.XarrayZarrRecipe`,
  permitting the bypassing of h5py when opening inputs. {pull}`218`

## v0.6.0 - 2021-09-02

- Added {class}`pangeo_forge_recipes.recipes.HDFReferenceRecipe` class to create virtual Zarrs from collections of
  NetCDF / HDF5 files. {pull}`174`
- Limit output from logging. {pull}`175`
- Change documentation structure. {pull}`178`
- Move `fsspec_open_kwargs` and `is_opendap` parameters
  out of {class}`pangeo_forge_recipes.recipes.XarrayZarrRecipe` and into
  {class}`pangeo_forge_recipes.patterns.FilePattern`. Add `query_string_secrets`
  as attribute of {class}`pangeo_forge_recipes.patterns.FilePattern`. {pull}`167`

## v0.5.0 - 2021-07-11

- Added `subset_inputs` option to {class}`pangeo_forge_recipes.recipes.XarrayZarrRecipe`. {issue}`93`, {pull}`166`
- Fixed file opening to eliminate HDF errors related to closed files. {issue}`170`, {pull}`171`
- Changed default behavior of executors so that the `cache_input` loop is always
  run, regardless of the value of `cache_inputs`. {pull}`168`

## v0.4.0 - 2021-06-25

- Fixed issue with recipe serialilzation causing high memory usage of Dask schedulers and workers when
  executing recipes with Prefect or Dask {pull}`160`.
- Added new methods `.to_dask()`, `to_prefect()`, and `.to_function()` for converting a recipe
  to one of the Dask, Prefect, or Python execution plans. The previous method, `recpie.to_pipelines()`
  is now deprecated.

## v0.3.4 - 2021-05-25

- Added `copy_pruned` method to {class}`pangeo_forge_recipes.recipes.XarrayZarrRecipe` to facilitate testing.
- Internal refactor of storage module.

## v0.3.3 - 2021-05-10

Many feature enhancements.
Non-backwards compatible changes to core API.
Package renamed from `pangeo_forge` to `pangeo_forge_recipes`.

There were problems with packaging for the 0.3.0-0.3.2 releases.

## v0.2.0 - 2021-04-26

First release since major Spring 2021 overhaul.
This release depends on Xarray v0.17.1, which has not yet been released as of the date of this release.

## v0.1.0 - 2020-10-22

First release.
