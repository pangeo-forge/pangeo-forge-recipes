from pkg_resources import DistributionNotFound, get_distribution

# from pangeo_forge.pipelines import AbstractPipeline

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:  # noqa: F401
    # package is not installed
    pass

del get_distribution, DistributionNotFound
