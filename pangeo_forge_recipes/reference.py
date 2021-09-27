"""
Functions related to creating fsspec references.
"""

from fsspec_reference_maker.hdf import SingleHdf5ToZarr


def create_hdf5_reference(fp, fname: str, inline_threshold: int = 300, **netcdf_storage_options):
    h5chunks = SingleHdf5ToZarr(fp, _unstrip_protocol(fname, fp.fs), inline_threshold=300)
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
