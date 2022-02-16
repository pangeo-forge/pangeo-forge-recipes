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

    from rich.logging import RichHandler

    logger = logging.getLogger("pangeo_forge_recipes")
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(getattr(logging, level))
    handler = RichHandler()
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
