"""
Functions related to creating fsspec references.
"""

import fsspec
from fsspec_reference_maker.hdf import SingleHdf5ToZarr


def create_hdf5_reference(fname: str, inline_threshold: int = 300, **netcdf_storage_options):
    with fsspec.open(fname, **netcdf_storage_options) as f:
        h5chunks = SingleHdf5ToZarr(f, _unstrip_protocol(fname, f.fs), inline_threshold=300)
        reference_data = h5chunks.translate()
    return reference_data


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
