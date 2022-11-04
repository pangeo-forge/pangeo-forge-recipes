"""
Functions related to creating fsspec references.
"""

from typing import Dict, Tuple, Union

from kerchunk.grib2 import scan_grib
from kerchunk.hdf import SingleHdf5ToZarr


def create_hdf5_reference(
    fp, fname: str, url: str, inline_threshold: int = 300, **netcdf_storage_options
) -> Dict:
    h5chunks = SingleHdf5ToZarr(fp, url, inline_threshold=inline_threshold)
    reference_data = h5chunks.translate()
    return reference_data


def create_grib2_reference(
    url: str, inline_threshold: int = 100, filter={}, **storage_options
) -> Dict:
    grib_references = scan_grib(
        url,
        storage_options=storage_options,
        inline_threshold=inline_threshold,
        filter=filter,
    )
    if not grib_references:
        raise ValueError("Provided filter did not return any messages")

    # Consolidate / post-process references
    if len(grib_references) == 1:
        ref = grib_references[0]
        ref["templates"] = {"u": url}
        return ref

    # 0) Pull out a sentinel to merge into
    ref = grib_references[0].copy()
    # 1) Template -> use url which encodes acces mechanism
    ref["templates"] = {"u": url}

    # 2) Copy all the references together
    primary_refs = ref["refs"].copy()
    for i, other_ref in enumerate(grib_references[1:]):
        print(f"Copying ref {i}")
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
