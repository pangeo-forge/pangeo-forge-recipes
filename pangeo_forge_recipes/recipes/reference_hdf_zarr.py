import functools
import json
import os
from dataclasses import dataclass, field
from typing import Callable, Dict, Hashable, Iterable, Optional

import fsspec
import yaml
from fsspec_reference_maker.combine import MultiZarrToZarr
from fsspec_reference_maker.hdf import SingleHdf5ToZarr

from ..patterns import FilePattern, Index
from ..storage import FSSpecTarget, MetadataTarget
from .base import BaseRecipe

ChunkKey = Index


def no_op(*_, **__) -> None:
    """A function that does nothing, regardless of inputs"""
    return None


@dataclass
class HDFReferenceRecipe(BaseRecipe):
    """
    Generates reference files for each input netCDF, then combines
    into one ensemble output

    Currently supports concat or merge along a single dimension.

    See fsspec-reference-maker and fsspec's ReferenceFileSystem.
    To use this class, you must have fsspec-reference-maker, ujson,
    xarray, fsspec, zarr, h5py and ujson in your recipe's requirements.

    This class will also produce an Intake catalog stub in YAML format
    You can use intake (and intake-xarray) to load the dataset
    This is the recommended way to distribute access.

    :param file_pattern: FilePattern describing the original data files.
        Paths should include protocol specifier, e.g. `https://`
    :param output_json_fname: The name of the json file in which to store the reference filesystem.
    :param output_intake_yaml_fname: The name of the generated intake catalog file.
    :param target: Final storage location in which to put the reference dataset files
        (json and yaml).
    :param metadata_cache: A location in which to cache metadata for files.
    :param netcdf_storage_options: dict of kwargs for creating fsspec
        instance to read original data files
    :param inline_threshold: blocks with fewer bytes than this will be
        inlined into the output reference file
    :param output_storage_options: dict of kwargs for creating fsspec
        instance when writing final output
    :param template_count: the number of occurrences of a URL before it
        gets made a template. ``None`` to disable templating
    :param xarray_open_kwargs: kwargs passed to xarray.open_dataset.
        Only used if `file_pattern` has more than one file.
    :param xarray_concat_args: kwargs passed to xarray.concat
        Only used if `file_pattern` has more than one file.
    """

    # TODO: support chunked ("tree") aggregation: would entail processing each file
    #  in one stage, running a set of merges in a second step and doing
    #  a master merge in finalise. This would maybe map to iter_chunk,
    #  store_chunk, finalize_target. The strategy was used for NWM's 370k files.

    # TODO: as written, this recipe is specific to HDF5 files. fsspec-reference-maker
    #  also supports TIFF and grib2 (and more coming)
    file_pattern: FilePattern
    output_json_fname: str = "reference.json"
    output_intake_yaml_fname: str = "reference.yaml"
    target: Optional[FSSpecTarget] = None
    metadata_cache: Optional[MetadataTarget] = None
    netcdf_storage_options: dict = field(default_factory=dict)
    inline_threshold: int = 500
    output_storage_options: dict = field(default_factory=dict)
    template_count: Optional[int] = 20
    xarray_open_kwargs: dict = field(default_factory=dict)
    xarray_concat_args: dict = field(default_factory=dict)

    def __post_init__(self):
        self._validate_file_pattern()

    def _validate_file_pattern(self):
        if len(self.file_pattern.merge_dims) > 1:
            raise NotImplementedError("This Recipe class can't handle more than one merge dim.")
        if len(self.file_pattern.concat_dims) > 1:
            raise NotImplementedError("This Recipe class can't handle more than one concat dim.")

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
        return no_op

    def iter_chunks(self) -> Iterable[Hashable]:
        """Iterate over all target chunks."""
        for input_key in self.file_pattern:
            yield input_key

    @property
    def store_chunk(self) -> Callable[[Hashable], None]:
        """Store a chunk of data in the target.
        """
        return functools.partial(
            _one_chunk,
            file_pattern=self.file_pattern,
            netcdf_storage_options=self.netcdf_storage_options,
            metadata_cache=self.metadata_cache,
        )

    @property
    def finalize_target(self) -> Callable[[], None]:
        """Final step to finish the recipe after data has been written.
        Attribute that returns a callable function.
        """
        proto = fsspec.utils.get_protocol(next(self.file_pattern.items())[1])
        concat_args = self.xarray_concat_args.copy()
        if "dim" in concat_args:
            raise ValueError(
                "Please do not set 'dim' in xarray_concat_args. "
                "It is determined automatically from the File Pattern."
            )
        concat_args["dim"] = self.file_pattern.concat_dims[0]  # there should only be one
        return functools.partial(
            _finalize,
            output_json_fname=self.output_json_fname,
            output_intake_yaml_fname=self.output_intake_yaml_fname,
            out_target=self.target,
            metadata_cache=self.metadata_cache,
            remote_protocol=proto,
            output_storage_options=self.output_storage_options,
            remote_options=self.netcdf_storage_options,
            xr_open_kwargs=self.xarray_open_kwargs,
            xr_concat_kwargs=concat_args,
            template_count=self.template_count,
        )


def _one_chunk(
    chunk_key: ChunkKey,
    file_pattern: FilePattern,
    netcdf_storage_options: Dict,
    metadata_cache: MetadataTarget,
):
    fname = file_pattern[chunk_key]
    with fsspec.open(fname, **netcdf_storage_options) as f:
        fn = os.path.basename(fname + ".json")
        h5chunks = SingleHdf5ToZarr(f, _unstrip_protocol(fname, f.fs), inline_threshold=300)
        metadata_cache[fn] = h5chunks.translate()


def _finalize(
    output_json_fname,
    output_intake_yaml_fname,
    out_target,
    metadata_cache,
    remote_protocol,
    output_storage_options,
    remote_options,
    xr_open_kwargs,
    xr_concat_kwargs,
    template_count,
):

    files = list(
        metadata_cache.getitems(list(metadata_cache.get_mapper())).values()
    )  # returns dicts from remote
    if len(files) == 1:
        out = files[0]
    else:
        mzz = MultiZarrToZarr(
            files,
            remote_protocol=remote_protocol,
            remote_options=remote_options,
            xarray_open_kwargs=xr_open_kwargs,
            xarray_concat_args=xr_concat_kwargs,
        )
        # mzz does not support directly writing to remote yet
        # get dict version and push it
        out = mzz.translate(None, template_count=template_count)
    fs = out_target.fs
    protocol = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
    with out_target.open(output_json_fname, mode="wt") as f:
        f.write(json.dumps(out))
    spec = {
        "sources": {
            "data": {
                "driver": "intake_xarray.xzarr.ZarrSource",
                "description": "",  # could derive from data attrs or recipe
                "args": {
                    "urlpath": "reference://",
                    "storage_options": {
                        "fo": out_target._full_path(output_json_fname),
                        "target_protocol": protocol,
                        "target_options": output_storage_options,
                        "remote_protocol": remote_protocol,
                        "remote_options": remote_options,
                        "skip_instance_cache": True,
                    },
                    "chunks": {},  # can optimize access here
                    "consolidated": False,
                },
            }
        }
    }
    with out_target.open(output_intake_yaml_fname, mode="wt") as f:
        yaml.dump(spec, f, default_flow_style=False)


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
