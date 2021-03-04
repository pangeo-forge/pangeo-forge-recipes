"""Example of a "NetCDF Zarr Multi-Variable Sequential Recipe" using Beam.
"""
import contextlib
import functools
import logging
from typing import Callable, List, Sequence, TypeVar

from absl import app
from absl import flags
import apache_beam as beam
import pandas as pd
import xarray

# Google-internal libaries
from XXX import gfile  # a library that can access either local or remote files
from XXX import zarr_gfile_store  # a zarr store built on top of gfile
from XXX import file_util


# pylint: disable=logging-format-interpolation

EXAMPLE_PATTERN = flags.DEFINE_multi_string(
    'example_pattern', '',
    'Comma-separated list of glob patterns for example netCDF files, which '
    'should contain all variables but not necessarily all times.')
INPUT_PATTERN = flags.DEFINE_multi_string(
    'input_pattern', '',
    'Comma-separated list of glob patterns for all netCDF files to convert.')
OUTPUT_PATH = flags.DEFINE_string(
    'output_path', None,
    'Path to destination zarr file.')
YEARS = flags.DEFINE_string(
    'years', '1900-2000',
    'Years (inclusive) found in input_pattern, separated by a dash.')
TIME_CHUNK_SIZE = flags.DEFINE_integer(
    'time_chunk_size', 24,
    'Number of hourly time-steps to include in a single chunk.')


FLAGS = flags.FLAGS


def _glob_patterns_to_paths(patterns: List[str]) -> List[str]:
  """Convert a list of glob patterns into paths."""
  paths = []
  for pattern in patterns:
    paths.extend(gfile.Glob(pattern))
  return paths


def _fix_data_glitches(path: str, ds: xarray.Dataset) -> xarray.Dataset:
  """Fix dataset specific inconsistencies, e.g., variable names or alignment."""
  ds = ds.copy(deep=False)
  ...
  return ds


@contextlib.contextmanager
def _open_netcdf(path, *, decode_cf=True):
  """Open a netCDF file from a remote path."""
  with file_util.copy_to_temp(path) as local_path:
    ds = xarray.open_dataset(local_path, decode_cf=decode_cf)
    ds = _fix_data_glitches(path, ds)
    yield ds


def _extract_template(path: str, time_chunk_size: int) -> xarray.Dataset:
  """Extract a dummy Dataset with metadata matching a dataset on disk."""
  with _open_netcdf(path) as full_ds:
    if 'time' in full_ds.dims:
      # Only include the first chunk worth of data.
      sample = full_ds.head(time=time_chunk_size).compute()
      # Replace time-dependent variables with dummies of all zeros using dask:
      # - This ensures the dataset remains small when serialized with pickle
      #   when passed between Beam stages.
      # - This ensures these variables won't get written to disk by to_zarr
      #   with compute=False.
      template = xarray.zeros_like(sample.chunk())
    else:
      template = full_ds.compute()
  return template


def _expand_time_dimension(
    dataset: xarray.Dataset,
    times: pd.DatetimeIndex,
) -> xarray.Dataset:
  """Expand an xarray.Dataset to use a new array of times."""
  old_size = dataset.sizes['time']
  repeats, remainder = divmod(times.size, old_size)
  if remainder:
    raise ValueError('new time dimension size must be a multiple of the old '
                     f'size: {times.size} vs {old_size} '
                     f'with time:\n{times}\nand dataset:\n{dataset}')
  expanded = xarray.concat([dataset] * repeats, dim='time', data_vars='minimal')
  expanded.coords['time'] = times
  return expanded


def _write_template_to_zarr(
    templates: Sequence[xarray.Dataset],
    times: pd.DatetimeIndex,
    zarr_store: zarr_gfile_store.GFileStore,
):
  """Create an empty Zarr file matching the given templates."""
  # strict merge: no broadcasting, alignment or inconsistent attrs allowed
  merged = xarray.merge(templates, compat='identical', join='exact')
  expanded = _expand_time_dimension(merged, times)
  # compute=False means don't write data saved in dask arrays
  expanded.to_zarr(zarr_store, compute=False, consolidated=True, mode='w')


def _copy_netcdf_to_zarr_region(
    netcdf_path: str, zarr_store: zarr_gfile_store.GFileStore, time_chunk_size: int,
):
  """Copy a netCDF file into a Zarr file."""
  reference = xarray.open_zarr(zarr_store, chunks=False)
  time = reference.indexes['time']

  with _open_netcdf(netcdf_path) as source_ds:
    start = source_ds.indexes['time'][0]
    end = source_ds.indexes['time'][-1]
    region = {'time': slice(time.get_loc(start), time.get_loc(end) + 1)}

    # coordinates were already written as part of the template
    source_ds = source_ds.drop(list(source_ds.dims) + list(source_ds.coords))
    source_ds = source_ds.chunk({'time': time_chunk_size})

    delayed = source_ds.to_zarr(zarr_store, region=region, compute=False)
    # use multiple threads
    delayed.compute(num_workers=16)


def main(unused_argv):
  example_paths = _glob_patterns_to_paths(EXAMPLE_PATTERN.value)
  input_paths = _glob_patterns_to_paths(INPUT_PATTERN.value)

  if not example_paths:
    raise RuntimeError('No example files')

  example_paths_string = '\n'.join(example_paths)
  logging.info(f'Example paths:\n{example_paths_string}')

  input_paths_string = '\n'.join(input_paths)
  logging.info(f'All input paths:\n{input_paths_string}')

  # This builds the time-coordinate on the desired dataset.
  start, end = YEARS.value.split('-')
  stop = str(int(end) + 1)
  times = pd.date_range(start, stop, freq='H', closed='left')

  zarr_store = zarr_gfile_store.GFileStore(OUTPUT_PATH.value)

  def pipeline(root):
    write_zarr_metadata = (
        root
        | 'example paths' >> beam.Create(example_paths)
        | 'extract templates' >> beam.Map(
            _extract_template, time_chunk_size=TIME_CHUNK_SIZE.value
        )
        | 'combine templates' >> beam.combiners.ToList()
        | 'write zarr template' >> beam.Map(
            _write_template_to_zarr, times=times, zarr_store=zarr_store,
        )
    )
    write_zarr_chunks = (  # pylint: disable=unused-variable
        root
        | 'input paths' >> beam.Create(input_paths)
        | 'wait on metadata' >> beam.Map(
            lambda path, _: path, beam.pvalue.AsSingleton(write_zarr_metadata),
        )
        | 'write array data' >> beam.Map(
            _copy_netcdf_to_zarr_region,
            zarr_store=zarr_store,
            time_chunk_size=TIME_CHUNK_SIZE.value,
        )
    )
  runner = beam.runners.DirectRunner()  # could swap this out, e.g., for DataflowRunner()
  runner.run(pipeline)


if __name__ == '__main__':
  app.run(main)
