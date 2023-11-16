from dataclasses import dataclass, field
from typing import Union, Optional

from pangeo_forge_recipes.storage import FSSpecTarget, CacheFSSpecTarget
from pangeo_forge_recipes.transforms import RequiredAtRuntimeDefault


@dataclass
class RecipeConfig:
    """a simple class that SHOULD house all injection spec values
    from `pangeo_forge_recipes.injections.py:get_injection_specs()`

    This way folks can use it to pass injection spec values around
    on to custom transforms or partials in their recipe:

    ```python
    from config import RecipeConfig

    @dataclass
    class MyCustomTransform(beam.PTransform):
        # note that `MyCustomTransform` is not part of the
        # pangeo_forge_recipes.injections.py:get_injection_specs()
        # but now it can be used anyway in practice
        target_root: None

    recipe_config = RecipeConfig()

    beam.Create() | MyCustomTransform(target_root=recipe_config.target_root)
    ```
    """
    target_root: Union[str, FSSpecTarget, RequiredAtRuntimeDefault] = field(
        default_factory=RequiredAtRuntimeDefault
    )
    cache: Optional[Union[str, CacheFSSpecTarget]] = ""
