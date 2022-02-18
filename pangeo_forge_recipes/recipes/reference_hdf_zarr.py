from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Hashable, Iterable, Optional

import fsspec
import yaml
from fsspec_reference_maker.combine import MultiZarrToZarr

from ..executors.base import Pipeline, Stage
from ..patterns import Index
from ..reference import create_hdf5_reference, unstrip_protocol
from ..storage import file_opener
from .base import BaseRecipe, FilePatternMixin, StorageMixin

ChunkKey = Index


def no_op(*_, **__) -> None:
    """A function that does nothing, regardless of inputs"""
    return None


def scan_file(chunk_key: ChunkKey, config: HDFReferenceRecipe):
    assert config.storage_config.metadata is not None, "metadata_cache is required"
    fname = config.file_pattern[chunk_key]
    ref_fname = os.path.basename(fname + ".json")
    with file_opener(fname, **config.netcdf_storage_options) as fp:
        protocol = getattr(getattr(fp, "fs", None), "protocol", None)  # make mypy happy
        if protocol is None:
            raise ValueError("Couldn't determine protocol")
        target_url = unstrip_protocol(fname, protocol)
        config.storage_config.metadata[ref_fname] = create_hdf5_reference(fp, target_url, fname)


def finalize(config: HDFReferenceRecipe):
    assert config.storage_config.target is not None, "target is required"
    assert config.storage_config.metadata is not None, "metadata_cache is required"
    remote_protocol = fsspec.utils.get_protocol(next(config.file_pattern.items())[1])
    concat_args = config.xarray_concat_args.copy()
    if "dim" in concat_args:
        raise ValueError(
            "Please do not set 'dim' in xarray_concat_args. "
            "It is determined automatically from the File Pattern."
        )
    concat_args["dim"] = config.file_pattern.concat_dims[0]  # there should only be one

    files = list(
        config.storage_config.metadata.getitems(
            list(config.storage_config.metadata.get_mapper())
        ).values()
    )  # returns dicts from remote
    if len(files) == 1:
        out = files[0]
    else:
        mzz = MultiZarrToZarr(
            files,
            remote_protocol=remote_protocol,
            remote_options=config.netcdf_storage_options,
            xarray_open_kwargs=config.xarray_open_kwargs,
            xarray_concat_args=concat_args,
        )
        # mzz does not support directly writing to remote yet
        # get dict version and push it
        out = mzz.translate(None, template_count=config.template_count)
    fs = config.storage_config.target.fs
    protocol = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
    with config.storage_config.target.open(config.output_json_fname, mode="wt") as f:
        f.write(json.dumps(out))
    spec = {
        "sources": {
            "data": {
                "driver": "intake_xarray.xzarr.ZarrSource",
                "description": "",  # could derive from data attrs or recipe
                "args": {
                    "urlpath": "reference://",
                    "storage_options": {
                        "fo": config.storage_config.target._full_path(config.output_json_fname),
                        "target_protocol": protocol,
                        "target_options": config.output_storage_options,
                        "remote_protocol": remote_protocol,
                        "remote_options": config.netcdf_storage_options,
                        "skip_instance_cache": True,
                    },
                    "chunks": {},  # can optimize access here
                    "consolidated": False,
                },
            }
        }
    }
    with config.storage_config.target.open(config.output_intake_yaml_fname, mode="wt") as f:
        yaml.dump(spec, f, default_flow_style=False)


def hdf_reference_recipe_compiler(recipe: HDFReferenceRecipe) -> Pipeline:
    stages = [
        Stage(name="scan_file", function=scan_file, mappable=list(recipe.iter_inputs())),
        Stage(name="finalize", function=finalize),
    ]
    return Pipeline(stages=stages, config=recipe)


@dataclass
class HDFReferenceRecipe(BaseRecipe, StorageMixin, FilePatternMixin):
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
    :param storage_config: Defines locations for writing the reference dataset files (json and yaml)
      and for caching metadata for files. Both locations default to ``tempdir.TemporaryDirectory``;
      this default config can be used for testing and debugging the recipe. In an actual execution
      context, the default config is re-assigned to point to the destination(s) of choice, which can
      be any combination of ``fsspec``-compatible storage backends.
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

    _compiler = hdf_reference_recipe_compiler

    # TODO: support chunked ("tree") aggregation: would entail processing each file
    #  in one stage, running a set of merges in a second step and doing
    #  a master merge in finalise. This would maybe map to iter_chunk,
    #  store_chunk, finalize_target. The strategy was used for NWM's 370k files.

    # TODO: as written, this recipe is specific to HDF5 files. fsspec-reference-maker
    #  also supports TIFF and grib2 (and more coming)
    output_json_fname: str = "reference.json"
    output_intake_yaml_fname: str = "reference.yaml"
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
        yield from self.file_pattern
