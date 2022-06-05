"""Standalone functions for opening sources as Dataset objects."""

from typing import Union, Any, Optional, Dict

import xarray as xr

from .patterns import FileType


OpenFile = Any  # could fsspec provide a stricter type here?


OPENER_MAP = {
    FileType.netcdf3: dict(engine="scipy"),
    FileType.netcdf4: dict(engine="h5netcdf"),
    FileType.zarr: dict(engine="zarr")
}

def _set_engine(file_type, xr_open_kwargs):
    kw = xr_open_kwargs.copy()
    if "engine" in kw:
        engine_message_base = (
            "pangeo-forge-recipes will automatically set the xarray backend for "
            f"files of type '{file_type.value}' to '{OPENER_MAP[file_type]}', "
        )
        warn_matching_msg = engine_message_base + (
            "which is the same value you have passed via `xarray_open_kwargs`. "
            f"If this input file is actually of type '{file_type.value}', you can "
            f"remove `{{'engine': '{kw['engine']}'}}` from `xarray_open_kwargs`. "
        )
        error_mismatched_msg = engine_message_base + (
            f"which is different from the value you have passed via "
            "`xarray_open_kwargs`. If this input file is actually of type "
            f"'{file_type.value}', please remove `{{'engine': '{kw['engine']}'}}` "
            "from `xarray_open_kwargs`. "
        )
        engine_message_tail = (
            f"If this input file is not of type '{file_type.value}', please update"
            " this recipe by passing a different value to `FilePattern.file_type`."
        )
        warn_matching_msg += engine_message_tail
        error_mismatched_msg += engine_message_tail

        if kw["engine"] == OPENER_MAP[file_type]["engine"]:
            warnings.warn(warn_matching_msg)
        elif kw["engine"] != OPENER_MAP[file_type]["engine"]:
            raise ValueError(error_mismatched_msg)
    else:
        kw.update(OPENER_MAP[file_type])
    return kw


def open_with_xarray(
    thing: Union[OpenFile, str],
    file_type: FileType = FileType.unknown,
    load: bool = False,
    xarray_open_kwargs: Optional[Dict] = None
) -> xr.Dataset:
    # TODO: check file type matrix

    kw = xarray_open_kwargs or {}
    kw = _set_engine(file_type, kw)
    # workaround fsspec inconsistencies
    if hasattr(thing, "open"):
        thing = thing.open()
    print(kw)
    ds = xr.open_dataset(thing, **kw)
    if load:
        ds.load()
    return ds
