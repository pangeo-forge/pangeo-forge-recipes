"""
Executors know how to run recipes.
"""

from .prefect import PrefectExecutor
from .python import PythonExecutor

__all__ = [PythonExecutor, PrefectExecutor]
