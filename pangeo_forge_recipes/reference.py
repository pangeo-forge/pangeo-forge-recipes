"""
Functions related to creating fsspec references.
"""

from typing import Dict, Optional, Tuple, Union

from kerchunk.grib2 import scan_grib
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.netCDF3 import NetCDF3ToZarr

from .patterns import FileType


def create_kerchunk_reference(
    fp,
    url: str,
    file_type: FileType,
    inline_threshold: int = 300,
    grib_filters: Optional[dict] = None,
) -> Dict:

    if file_type == FileType.netcdf4:
        chunks = SingleHdf5ToZarr(fp, url, inline_threshold=inline_threshold)
        ref = chunks.translate()

    elif file_type == FileType.netcdf3:
        chunks = NetCDF3ToZarr(url, max_chunk_size=100_000_000)
        ref = chunks.translate()
    elif file_type == FileType.grib:

        grib_references = scan_grib(
            url,
            inline_threshold=inline_threshold,
            filter=grib_filters,
        )

        # Consolidate / post-process references
        if len(grib_references) == 1:
            ref = grib_references[0]
            ref["templates"] = {"u": url}
            return ref

        ref = grib_references[0].copy()
        ref["templates"] = {"u": url}

        primary_refs = ref["refs"].copy()
        for i, other_ref in enumerate(grib_references[1:]):
            primary_refs.update(other_ref["refs"])
        ref["refs"] = primary_refs

    return ref


def unstrip_protocol(name: str, protocol: Union[str, Tuple[str, ...]]) -> str:
    # should be upstreamed into fsspec and maybe also
    # be a method on an OpenFile
    if isinstance(protocol, str):
        if name.startswith(protocol):
            return name
        return protocol + "://" + name
    else:
        if name.startswith(tuple(protocol)):
            return name
        return protocol[0] + "://" + name
