"""
Functions related to creating fsspec references.
"""

from typing import Dict, Tuple, Union

from kerchunk.hdf import SingleHdf5ToZarr

from .patterns import FileType


def create_kerchunk_reference(
    fp, url: str, file_type: FileType, inline_threshold: int = 300
) -> Dict:
    if file_type == FileType.netcdf4:
        chunks = SingleHdf5ToZarr(fp, url, inline_threshold=inline_threshold)
    elif file_type == FileType.netcdf3:
        from kerchunk.netCDF3 import NetCDF3ToZarr

        chunks = NetCDF3ToZarr(url, max_chunk_size=100_000_000)
    return chunks.translate()


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
