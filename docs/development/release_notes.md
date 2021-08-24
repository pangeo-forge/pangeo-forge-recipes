# Release Notes

## v0.5.1 - Unreleased


## v0.5.0 - 2021-07-11

- Added `subset_inputs` option to `XarrayZarrRecipe`. {issue}`93`, {pull}`166`
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

- Added `copy_pruned` method to `XarrayZarrRecipe` to facilitate testing.
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
