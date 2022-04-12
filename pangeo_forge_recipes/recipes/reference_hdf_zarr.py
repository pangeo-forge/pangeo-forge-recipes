from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Hashable, Iterable, Optional, Dict, List

import apache_beam as beam
import fsspec
import yaml
from fsspec_reference_maker.combine import MultiZarrToZarr

from .base import BaseRecipe, FilePatternMixin
from ..executors.base import Pipeline, Stage
from ..patterns import Index, OpenPattern, FileNames
from ..reference import create_hdf5_reference, unstrip_protocol
from ..storage import FSSpecTarget, MetadataTarget, file_opener

ChunkKey = Index


def no_op(*_, **__) -> None:
    """A function that does nothing, regardless of inputs"""
    return None


def scan_file(chunk_key: ChunkKey, config: HDFReferenceRecipe):
    assert config.metadata_cache is not None, "metadata_cache is required"
    fname = config.file_pattern[chunk_key]
    reference = scan_file_pure(fname, config)
    ref_fname = os.path.basename(fname + ".json")
    config.metadata_cache[ref_fname] = reference


def scan_file_pure(fname: str, config: HDFReferenceRecipe) -> Dict:
    with file_opener(fname, **config.netcdf_storage_options) as fp:
        protocol = getattr(getattr(fp, "fs", None), "protocol", None)  # make mypy happy
        if protocol is None:
            raise ValueError("Couldn't determine protocol")
        target_url = unstrip_protocol(fname, protocol)
        return create_hdf5_reference(fp, url=target_url, fname=fname)


def finalize(config: HDFReferenceRecipe):
    assert config.metadata_cache is not None, "metadata_cache is required"
    files = list(
        config.metadata_cache.getitems(list(config.metadata_cache.get_mapper())).values()
    )  # returns dicts from remote
    finalize_pure(files, config)


def finalize_pure(files: List[Dict], config: HDFReferenceRecipe) -> None:
    assert config.target is not None, "target is required"
    remote_protocol = fsspec.utils.get_protocol(next(config.file_pattern.items())[1])
    concat_args = config.xarray_concat_args.copy()
    if "dim" in concat_args:
        raise ValueError(
            "Please do not set 'dim' in xarray_concat_args. "
            "It is determined automatically from the File Pattern."
        )
    concat_args["dim"] = config.file_pattern.concat_dims[0]  # there should only be one

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
    fs = config.target.fs
    protocol = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
    with config.target.open(config.output_json_fname, mode="wt") as f:
        f.write(json.dumps(out))
    spec = {
        "sources": {
            "data": {
                "driver": "intake_xarray.xzarr.ZarrSource",
                "description": "",  # could derive from data attrs or recipe
                "args": {
                    "urlpath": "reference://",
                    "storage_options": {
                        "fo": config.target._full_path(config.output_json_fname),
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
    with config.target.open(config.output_intake_yaml_fname, mode="wt") as f:
        yaml.dump(spec, f, default_flow_style=False)


def hdf_reference_recipe_compiler(recipe: HDFReferenceRecipe) -> Pipeline:
    stages = [
        Stage(name="scan_file", function=scan_file, mappable=list(recipe.iter_inputs())),
        Stage(name="finalize", function=finalize),
    ]
    return Pipeline(stages=stages, config=recipe)


@dataclass
class ScanFiles(beam.PTransform):
    config: BaseRecipe

    def expand(self, pcoll):
        return pcoll | beam.Map(scan_file_pure, config=self.config)


@dataclass
class WriteZarrReference(beam.PTransform):
    config: BaseRecipe

    def expand(self, pcoll):
        return (
                pcoll
                | beam.combiners.ToList()
                | beam.Map(finalize_pure, config=self.config)
        )


@dataclass
class HDFReferenceRecipe(BaseRecipe, FilePatternMixin):
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

    _compiler = hdf_reference_recipe_compiler

    # TODO: support chunked ("tree") aggregation: would entail processing each file
    #  in one stage, running a set of merges in a second step and doing
    #  a master merge in finalise. This would maybe map to iter_chunk,
    #  store_chunk, finalize_target. The strategy was used for NWM's 370k files.

    # TODO: as written, this recipe is specific to HDF5 files. fsspec-reference-maker
    #  also supports TIFF and grib2 (and more coming)
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
        yield from self.file_pattern

    def to_beam(self):
        return OpenPattern(self.file_pattern) | FileNames() | ScanFiles(self) | WriteZarrReference(self)
