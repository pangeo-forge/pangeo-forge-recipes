import itertools

import numpy as np


# https://alexwlchan.net/2018/12/iterating-in-fixed-size-chunks/
def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


# only needed because of
# https://github.com/pydata/xarray/issues/4631
def fix_scalar_attr_encoding(ds):
    def _fixed_attrs(d):
        fixed = {}
        for k, v in d.items():
            if isinstance(v, np.ndarray) and len(v) == 1:
                fixed[k] = v[0]
        return fixed

    ds = ds.copy()
    ds.attrs.update(_fixed_attrs(ds.attrs))
    ds.encoding.update(_fixed_attrs(ds.encoding))
    for v in ds.variables:
        ds[v].attrs.update(_fixed_attrs(ds[v].attrs))
        ds[v].encoding.update(_fixed_attrs(ds[v].encoding))
    return ds
