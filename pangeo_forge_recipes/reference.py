"""
Functions related to creating fsspec references.
"""

from typing import Dict, Tuple, Union

# NOTE(darothen): this class was added in July, 2022 (commit d185da9e5ca9b)
from kerchunk.grib2 import scan_grib
from kerchunk.hdf import SingleHdf5ToZarr


def create_hdf5_reference(
    fp, fname: str, url: str, inline_threshold: int = 300, **netcdf_storage_options
) -> Dict:
    h5chunks = SingleHdf5ToZarr(fp, url, inline_threshold=inline_threshold)
    reference_data = h5chunks.translate()
    return reference_data


def create_grib2_reference(
    fp, fname: str, url: str, inline_threshold: int = 100, **storage_options
) -> Dict:
    """
    NOTES:
    - if accessing over HTTPS, only use "skip_instance_cache"
    - S3 doesn't immediately work but it's not clear why.
    """
    # _storage_options = {
    #     # 'anon': True,
    #     'anon': False,
    #     'skip_instance_cache': True,
    #     # 'default_cache_type': 'readahead',
    # }
    # _storage_options.update(storage_options)
    # _common = ['time', 'step', 'latitude', 'longitude', 'valid_time']
    _common = storage_options.pop("common_coords", [])
    _filter = storage_options.pop("filter_by_keys", {})
    # _filter = {
    #     # 'typeOfLevel': 'isobaricInhPa',
    #     # 'level': 850,

    #     'typeOfLevel': 'surface',
    #     'shortName': 't',

    #     # 'typeOfLevel': 'heightAboveGround',
    #     # 'level': 2
    # }

    grib_references = scan_grib(
        # NOTE: fname works here but I think 'fp' is more appropriate since it's
        # the wrapper to open things. Butth at means that later, we need to
        # fix the final reference, since it will have a reference to the
        # wrapper which isn't JSON-serializable
        fp,  # fname, # fp,
        # common_vars=_common,
        common=_common,
        storage_options=storage_options,
        # NOTE(darothen): "inline_threashold" is mispelled this way upstream in
        #                 kerchunk.grib2.scan_grib()
        # NOTE(darothen): Fixed at head by fsspec/kerchunk#198
        # inline_threashold=inline_threshold,
        inline_threshold=inline_threshold,
        filter=_filter,
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

    # OLD - just fix url in templates
    # for ref in grib_references:
    #     templates = ref['templates']
    #     templates['u'] = url
    # return grib_references
    # NOTE: This doesn't work because each individual grib message is mapped as
    # an individual message. Some gross re-plumbing of the HDFReference class
    # would make this work, but for now we hack into the existing interface by
    # consolidating all the references for each grib message into a single
    # one above

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
