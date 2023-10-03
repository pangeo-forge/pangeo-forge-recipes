# Release Notes

## v0.10.3 - 2023-10-03

- Assign injection spec values for command line interface {pull}`566`
- Docs rewrite {pull}`610`
- Simplify reference recipe composition by nesting transforms {pull}`635`
- Reference transforms dependency and bugfixes {pull}`631` {pull}`632`

## v0.10.2 - 2023-09-19

- Fix bug preventing use of multiple merge dims {pull}`591`
- Add parquet storage target for reference recipes {pull}`620`
- Support addition of dataset-level attrs for zarr recipes {pull}`622`
- Integration testing upgrades {pull}`590` {pull}`605` {pull}`607` {pull}`611`
- Add missing `py.typed` marker for mypy compatibility {pull}`613`

## v0.10.1 - 2023-08-31

- Add sentinel as default for transform keyword arguments that are required at runtime and which
recipe developers may not want to set in recipe modules. This allows recipe modules to be importable
(i.e., unit-testable) and type-checkable during development. {pull}`588`
- `StoreToZarr` now emits a `zarr.storage.FSStore` which can be consumed by downstream transforms.
This is useful for opening and testing the completed zarr store, adding it to a catalog, etc. {pull}`574`
- Concurrency limiting transform added. This base transform can be used to limit concurrency for
calls to external services. It is now used internally to allow `OpenURLWithFSSpec` to be limited
to a specified maximum concurrency. {pull}`557`
- Various packaging, testing, and maintenance upgrades {pull}`565` {pull}`567` {pull}`576`
- Patched deserialization bug that affected rechunking on GCP Dataflow {pull}`548`

## v0.10.0 - 2023-06-30

- **Major breaking change:** This release represents a nearly complete rewrite of
the package, removing the custom recipe constructor classes and executors, and
replacing them with a set of modular, domain-specific Apache Beam `PTransform`s,
which can be flexibly composed and executed on any Apache Beam runner. The documention has
been updated to reflect this change. As the first release following this major
rewrite, we expect bugs and documentation gaps to exist, and we look forward to
community support in finding and triaging those issues. A blog post and further
documentaion of the motivations for and opportunities created by this major
change is forthcoming.

## v0.9 - 2022-05-11

- **Breaking changes:** Deprecated `XarrayZarrRecipe` manual stage methods.
Also deprecated `FilePattern(..., is_opendap=True)` kwarg, which is superseded by
`FilePattern(..., file_type="opendap")`. {pull}`362`
- Added `serialization` module along with `BaseRecipe.sha256` and `FilePattern.sha256` methods.
Collectively, this provides for generation of deterministic hashes for both recipe and file
pattern instances. Checking these hashes against those from a prior version of the recipe can be
used to determine whether or not a particular recipe instance in a Python module (which may
contain arbitrary numbers of recipe instances) has changed since the last time the instances in
that module were executed. The file pattern hashes are based on a merkle tree built cumulatively
from all of the index:filepath pairs yielded by the pattern's `self.items()` method. As such, in
cases where a new pattern is intended to append to an existing dataset which was built from a
prior version of that pattern, the pattern hash can be used to determine the index from which to
begin appending. This is demonstrated in the tests. {pull}`349`
- Created new Prefect executor which wraps the Dask executor in a single Task.
This should mitigate problems related to large numbers of Prefect Tasks ({issue}`347`).
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
