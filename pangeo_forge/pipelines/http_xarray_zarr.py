from prefect import Flow

from ..utils import chunked_iterable

from ..tasks.http import download
from ..tasks.xarray import combine_and_write
from ..tasks.zarr import consolidate_metadata

class HttpXarrayZarrMixin:

    @property
    def flow(self):

        if len(self.targets) == 1:
            target = self.targets[0]
        else:
            raise ValueError('Zarr target requires self.targets be a length one list')

        with Flow(self.name) as _flow:

            cached_sources = [download(k, self.cache_location) for k in self.sources]

            first = True
            write_tasks = []
            for source_group in chunked_iterable(cached_sources, self.files_per_chunk):
                write_task = combine_and_write(
                    source_group, target, self.append_dim, self.concat_dim, first=first
                )
                write_tasks.append(write_task)
                first = False
            cm = consolidate_metadata(target)

        # create dependencies in imperative mode
        for n in range(1, len(write_tasks)):
            write_tasks[n].set_upstream(write_tasks[n - 1], flow=_flow)
        cm.set_upstream(write_tasks[-1], flow=_flow)

        return _flow
