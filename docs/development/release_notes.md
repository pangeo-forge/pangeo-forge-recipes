# Release Notes

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
