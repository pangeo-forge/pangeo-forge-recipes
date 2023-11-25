from dataclasses import dataclass, field
from typing import Optional, Union

from pangeo_forge_recipes.storage import CacheFSSpecTarget, FSSpecTarget
from pangeo_forge_recipes.transforms import RequiredAtRuntimeDefault


@dataclass
class Config:
    """a class that contains all dependency injections needed for transforms
    """

    target_storage: Union[str, FSSpecTarget, RequiredAtRuntimeDefault] = field(
        default_factory=RequiredAtRuntimeDefault
    )
    input_cache_storage: Optional[Union[str, CacheFSSpecTarget]] = ""
