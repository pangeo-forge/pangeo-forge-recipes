from dataclasses import dataclass
import fsspec


@dataclass
class Target:
    """Representation of a storage target for Pangeo Forge.
    Attributes
    ----------
    url : FileSystemSpec.AbtractFileSystem
        The filesystem we are writing to. Should be instantiated outside this
        class.
    path : str
        The path where the target data will be saved.
    """

    fs: fsspec.AbstractFileSystem
    path: str

    def get_mapper(self):
        # don't want to use this because we want to use a fancier Zarr FSStore
        return self.fs.get_mapper(self.path)
