"""
Functions related to creating fsspec references.
"""

from typing import Dict, Tuple, Union

from kerchunk.grib2 import scan_grib
from kerchunk.hdf import SingleHdf5ToZarr


def create_hdf5_reference(*, fp, url: str, inline_threshold: int = 300) -> Dict:
    h5chunks = SingleHdf5ToZarr(fp, url, inline_threshold=inline_threshold)
    reference_data = h5chunks.translate()
    return reference_data


def create_grib_reference(*, fp, inline_threshold=300, **grib_filters) -> dict:
    grib_chunks = scan_grib(fp, inline_threshold=inline_threshold, filter=grib_filters)
    reference_data = grib_chunks.translate()
    return reference_data


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
