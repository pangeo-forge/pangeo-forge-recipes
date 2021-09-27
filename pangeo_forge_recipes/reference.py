"""
Functions related to creating fsspec references.
"""

from typing import Dict, Tuple, Union

from fsspec_reference_maker.hdf import SingleHdf5ToZarr


def create_hdf5_reference(
    fp, fname: str, url: str, inline_threshold: int = 300, **netcdf_storage_options
) -> Dict:
    h5chunks = SingleHdf5ToZarr(fp, url, inline_threshold=inline_threshold)
    reference_data = h5chunks.translate()
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
