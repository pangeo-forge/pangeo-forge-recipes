from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Callable, Hashable, Iterable, Optional

import fsspec
import yaml
from kerchunk.combine import MultiZarrToZarr

from ..executors.base import Pipeline, Stage
from ..patterns import FileType, Index
from ..reference import create_grib_reference, create_hdf5_reference, unstrip_protocol
from ..storage import file_opener
from .base import BaseRecipe, FilePatternMixin, StorageMixin

ChunkKey = Index


def no_op(*_, **__) -> None:
    """A function that does nothing, regardless of inputs"""
    return None


def scan_file(chunk_key: ChunkKey, config: ReferenceRecipe):
    assert config.storage_config.metadata is not None, "metadata_cache is required"
    fname = config.file_pattern[chunk_key]
    ref_fname = os.path.basename(fname + ".json")
    with file_opener(fname, **config.storage_options) as fp:
        protocol = getattr(getattr(fp, "fs", None), "protocol", None)  # make mypy happy
        if protocol is None:
            raise ValueError("Couldn't determine protocol")
        target_url = unstrip_protocol(fname, protocol)

        if config.file_pattern.file_type is FileType.netcdf4:
            config.storage_config.metadata[ref_fname] = create_hdf5_reference(fp=fp, url=target_url)
        elif config.file_pattern.file_type is FileType.grib:
            config.storage_config.metadata[ref_fname] = create_grib_reference(fp=fp)


def finalize(config: ReferenceRecipe):
    assert config.storage_config.target is not None, "target is required"
    assert config.storage_config.metadata is not None, "metadata_cache is required"
    remote_protocol = fsspec.utils.get_protocol(next(config.file_pattern.items())[1])

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
            remote_options=config.storage_options,
            target_options=config.target_options,
            coo_dtypes=config.coo_dtypes,
            coo_map=config.coo_map,
            identical_dims=config.identical_dims,
            concat_dims=config.file_pattern.concat_dims,
            preprocess=config.preprocess,
            postprocess=config.postprocess,
        )
        # mzz does not support directly writing to remote yet
        # get dict version and push it
        out = mzz.translate()
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
                        "remote_options": config.storage_options,
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


def reference_recipe_compiler(recipe: ReferenceRecipe) -> Pipeline:
    stages = [
        Stage(name="scan_file", function=scan_file, mappable=list(recipe.iter_inputs())),
        Stage(name="finalize", function=finalize),
    ]
    return Pipeline(stages=stages, config=recipe)


@dataclass
class ReferenceRecipe(BaseRecipe, StorageMixin, FilePatternMixin):
    """
    Generates reference files for each input file, then combines
    into one ensemble output

    Currently supports concat or merge along a single dimension.

    See kerchunk and fsspec's ReferenceFileSystem.
    To use this class, you must have kerchunk, ujson,
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
    # Update this
    :param storage_options: dict of kwargs for creating fsspec
        instance to read original data files
    :param target_options: dict of kwargs for creating fsspec
        instance to read Kerchunk reference files
    :param inline_threshold: blocks with fewer bytes than this will be
        inlined into the output reference file
    :param output_storage_options: dict of kwargs for creating fsspec
        instance when writing final output
    :param identical_dims: coordiate-like variables that are assumed to be the
        same in every input, and so do not need any concatenation
    :param coo_map: set of "selectors" defining how to fetch the dimension coordinates
        of any given chunk for each of the concat dimes. By default, this is the variable
        in the dataset with the same name as the given concat dim, except for "var",
        where the default is the name of each input variable. See the doc of
        MultiZarrToZarr for more details on possibilities.
    :param coo_dtyes: optional coercion of coordinate values before write
        Note that, if using cftime to read coordinate values, output will also be
        encoded with cftime (i.e., ints and special attributes) unless you specify
        an "M8[*]" as the output type.
    :param preprocess: a function applied to each HDF file's references before combine
    :param postprocess: a function applied to the global combined references before write
    """

    dataset_type = "kerchunk"
    _compiler = reference_recipe_compiler

    # TODO: support chunked ("tree") aggregation: would entail processing each file
    #  in one stage, running a set of merges in a second step and doing
    #  a master merge in finalise. This would maybe map to iter_chunk,
    #  store_chunk, finalize_target. The strategy was used for NWM's 370k files.

    output_json_fname: str = "reference.json"
    output_intake_yaml_fname: str = "reference.yaml"
    storage_options: dict = field(default_factory=dict)
    target_options: Optional[dict] = field(default_factory=dict)
    inline_threshold: int = 500
    output_storage_options: dict = field(default_factory=dict)
    concat_dims: list = field(default_factory=list)
    identical_dims: Optional[list] = field(default_factory=list)
    coo_map: Optional[dict] = field(default_factory=dict)
    coo_dtypes: Optional[dict] = field(default_factory=dict)
    preprocess: Optional[Callable] = None
    postprocess: Optional[Callable] = None

    def __post_init__(self):
        super().__post_init__()
        self._validate_file_pattern()

    # For FileType Validation:
    # if FileType is not in ['netcdf4', 'grib2']:
    # raise filetype note supported

    # After filetype validation, check FileType and change kerchunk inputs depending on which one.

    def _validate_file_pattern(self):
        if self.file_pattern.file_type not in [FileType.netcdf4, FileType.grib]:
            raise ValueError("This recipe works on netcdf4 or grib2 input only")
        if len(self.file_pattern.merge_dims) > 1:
            raise NotImplementedError("This Recipe class can't handle more than one merge dim.")
        if len(self.file_pattern.concat_dims) > 1:
            raise NotImplementedError("This Recipe class can't handle more than one concat dim.")

    def iter_inputs(self) -> Iterable[Hashable]:
        yield from self.file_pattern
