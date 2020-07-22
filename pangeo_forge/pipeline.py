import os
from abc import ABC, abstractmethod
from typing import List

import fsspec
import zarr
from prefect import Flow, task

from .utils import chunked_iterable


class AbstractPipeline(ABC):
    @property
    @abstractmethod
    def sources(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def targets(self) -> List[str]:
        pass

    @abstractmethod
    def run(self) -> None:
        pass


class XarrayPrefectPipelineMixin:
    @task
    def download(self, source_url):
        target_url = os.path.join(self.cache_location, str(hash(source_url)))

        # there is probably a better way to do caching!
        try:
            fsspec.open(target_url).open()
            return target_url
        except FileNotFoundError:
            pass

        with fsspec.open(source_url, mode="rb") as source:
            with fsspec.open(target_url, mode="wb") as target:
                target.write(source.read())
        return target_url

    @task
    def combine_and_write(self, sources, target, append_dim, first=True):
        import xarray as xr

        # while debugging this, I had itermittent fsspec / hdf5 read errors related to
        # "trying to read from a closed file"
        # but they seem to have gone away for now
        double_open_files = [fsspec.open(url).open() for url in sources]
        ds = xr.open_mfdataset(double_open_files, combine="nested", concat_dim=self.concat_dim)
        # by definition, this should be a contiguous chunk
        ds = ds.chunk({append_dim: len(sources)})

        if first:
            kwargs = dict(mode="w")
        else:
            kwargs = dict(mode="a", append_dim=append_dim)

        mapper = fsspec.get_mapper(target)
        ds.to_zarr(mapper, **kwargs)

    @task
    def consolidate_metadata(self):
        mapper = fsspec.get_mapper(self.targets)
        zarr.consolidate_metadata(mapper)

    @property
    def flow(self) -> Flow:
        with Flow("Pangeo-Forge") as flow:

            cached_sources = [self.download(k) for k in self.sources]

            first = True
            write_tasks = []
            for source_group in chunked_iterable(cached_sources, self.files_per_chunk):
                write_task = self.combine_and_write(
                    source_group, self.targets, self.concat_dim, first=first
                )
                write_tasks.append(write_task)
                first = False
            cm = self.consolidate_metadata(self.targets)

        # create dependencies in imperative mode
        for n in range(1, len(write_tasks)):
            write_tasks[n].set_upstream(write_tasks[n - 1], flow=flow)
        cm.set_upstream(write_tasks[-1], flow=flow)

        return flow

    def run(self):
        self.flow.run()
