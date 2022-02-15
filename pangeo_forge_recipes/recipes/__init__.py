from .reference_hdf_zarr import HDFReferenceRecipe
from .xarray_zarr import XarrayZarrRecipe

__all__ = ["XarrayZarrRecipe", "HDFReferenceRecipe"]


def setup_logging(level: str = "INFO"):
    """A convenience function that sets up logging for developing and debugging recipes in Jupyter,
    iPython, or another interactive context.

    :param level: One of (in decreasing level of detail) ``"DEBUG"``, ``"INFO"``, or ``"WARNING"``.
      Defaults to ``"INFO"``.
    """

    import logging
    import sys

    formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("pangeo_forge_recipes")
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(getattr(logging, level))
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
