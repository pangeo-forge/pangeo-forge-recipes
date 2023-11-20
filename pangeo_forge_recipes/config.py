from dataclasses import dataclass, field
from typing import Optional, Union

from pangeo_forge_recipes.storage import CacheFSSpecTarget, FSSpecTarget
from pangeo_forge_recipes.transforms import RequiredAtRuntimeDefault


@dataclass
class Config:
    """a simple class that contains all possible injection spec values

    folks can import it into their recipes to pass injection spec values
    around to custom beam routines as the example below shows

    Examples:
        ```python
        from config import Config

        @dataclass
        class MyCustomTransform(beam.PTransform):

            # this custom transform class is not listed
            # in `pangeo_forge_recipes.injections.py:get_injection_specs()`
            # and the instance attr will therefore not be dependency injected
            # but we can use the `config.Config` to pass around the injections
            target_root: None

            def expand(self,
                pcoll: beam.PCollection[Tuple[Index, xr.Dataset]],
            ) ->  beam.PCollection[Tuple[Index, xr.Dataset]]:
                return pcoll

        config = Config()
        recipe = (beam.Create() | MyCustomTransform(target_storage=config.target_storage))
        ```
    """

    target_storage: Union[str, FSSpecTarget, RequiredAtRuntimeDefault] = field(
        default_factory=RequiredAtRuntimeDefault
    )
    input_cache_storage: Optional[Union[str, CacheFSSpecTarget]] = ""
