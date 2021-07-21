import functools
import json
import os
import tempfile
from dataclasses import dataclass, field
from typing import Callable, Hashable, Iterable, List, Union

import fsspec
from fsspec_reference_maker.combine import MultiZarrToZarr
from fsspec_reference_maker.hdf import SingleHdf5ToZarr

from .base import BaseRecipe


def no_op(*_, **__) -> None:
    return None


@dataclass
class ReferenceHDFRecipe(BaseRecipe):
    """
    Generates reference files for each input netCDF, then combines
    into one ensemble output

    Currently supports concat or merge along a single dimension.
    TODO: multiple could be made by calling MultiZarrToZarr once
     for each dimension

    See fsspec-reference-maker and fsspec's ReferenceFileSystem

    SingleHdf5ToZarr parameters:

    :param netcdf_url: location of the original data files. Can be a
        string (will be glob-expanded) or explicit list of paths or
        list of glob-strings. May eventually support FilePatterns.
        Should include protocol specifier, as usually formulated for
        fsspec.
    :param netcdf_storage_options: dict of kwargs for creating fsspec
        instance to read original data files
    :param inline_threshold: blocks with fewer bytes than this will be
        inlined into the output reference file

    MultiZarrToZarr parameters:

    :param output_url: where the final reference file is written.
    :param output_storage_options: dict of kwargs for creating fsspec
        instance when writing final output
    :param xarray_open_kwargs: kwargs passed to xarray.open_dataset
    :param xarray_concat_args: kwargs passed to xarray.concat
    """

    netcdf_url: Union[str, List[str]]
    output_url: str
    _work_dir: str
    """temporary store for JSONs"""
    netcdf_storage_options: dict = field(default_factory=dict)
    inline_threshold: int = 500
    output_storage_options: dict = field(default_factory=dict)
    xarray_open_kwargs: dict = field(default_factory=dict)
    xarray_concat_args: dict = field(default_factory=dict)

    def iter_inputs(self) -> Iterable[Hashable]:
        return ()

    @property
    def cache_input(self) -> Callable[[Hashable], None]:
        return no_op

    @property
    def prepare_target(self) -> Callable[[], None]:
        """Prepare the recipe for execution by initializing the target.
        Attribute that returns a callable function.
        """
        self._work_dir = tempfile.mkdtemp()
        return no_op

    def iter_chunks(self) -> Iterable[Hashable]:
        """Iterate over all target chunks."""
        return fsspec.open_files(self.netcdf_url, **self.netcdf_storage_options)

    @property
    def store_chunk(self) -> Callable[[Hashable], None]:
        """Store a chunk of data in the target.
        """
        return functools.partial(_one_chunk, work_dir=self._work_dir)

    @property
    def finalize_target(self) -> Callable[[], None]:
        """Final step to finish the recipe after data has been written.
        Attribute that returns a callable function.
        """
        return functools.partial(
            _finalize,
            work_dir=self._work_dir,
            out_url=self.output_url,
            out_so=self.output_storage_options,
            remote_protocol=fsspec.utils.get_protocol(self.netcdf_url),
            remote_options=self.netcdf_storage_options,
            xr_open_kwargs=self.xarray_open_kwargs,
            xr_concat_kwargs=self.xarray_concat_args,
        )


def _one_chunk(of, work_dir):
    with of as f:
        fn = os.path.join(work_dir, os.path.basename(f.name + ".json"))
        h5chunks = SingleHdf5ToZarr(f, _unstrip_protocol(f.name, f.fs), inline_threshold=300)
        with open(fn, "w") as outf:
            json.dump(h5chunks.translate(), outf)


def _finalize(
    work_dir, out_url, out_so, remote_protocol, remote_options, xr_open_kwargs, xr_concat_kwargs
):
    files = os.listdir(work_dir)
    mzz = MultiZarrToZarr(
        files,
        remote_protocol=remote_protocol,
        remote_options=remote_options,
        xarray_open_kwargs=xr_open_kwargs,
        xarray_concat_args=xr_concat_kwargs,
    )
    fn = os.path.join(work_dir, "combined.json")
    # mzz does not support directly writing to remote yet
    mzz.translate(fn)
    fs = fsspec.core.url_to_fs(out_url, **out_so)
    fs.put(fn, out_url)


def _unstrip_protocol(name, fs):
    # should be upstreamed into fsspec and maybe also
    # be a method on an OpenFile
    if isinstance(fs.protocol, str):
        if name.startswith(fs.protocol):
            return name
        return fs.protocol + "://" + name
    else:
        if name.startswith(tuple(fs.protocol)):
            return name
        return fs.protocol[0] + "://" + name
