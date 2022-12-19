from .base import BaseRecipe
from .reference_zarr import ReferenceRecipe
from .xarray_zarr import XarrayZarrRecipe

__all__ = ["BaseRecipe", "XarrayZarrRecipe", "ReferenceRecipe"]


def setup_logging(level: str = "INFO"):
    """A convenience function that sets up logging for developing and debugging recipes in Jupyter,
    iPython, or another interactive context.

    :param level: One of (in decreasing level of detail) ``"DEBUG"``, ``"INFO"``, or ``"WARNING"``.
      Defaults to ``"INFO"``.
    """
    import logging

    try:
        from rich.logging import RichHandler

        handler = RichHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
    except ImportError:
        import sys

        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(logging.Formatter("%(name)s - %(levelname)s - %(message)s"))

    logger = logging.getLogger("pangeo_forge_recipes")
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(getattr(logging, level))
    logger.addHandler(handler)
