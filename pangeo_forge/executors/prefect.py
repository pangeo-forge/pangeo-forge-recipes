from typing import Any, Iterable, Tuple

import prefect

from ..recipe import DatasetRecipe

class PrefectExecutor():

    def prepare_plan(self, r: DatasetRecipe) -> prefect.Flow:

        # wrap our functions as prefect tasks
        @prefect.task
        def prepare() -> None:
            r.prepare()

        @prefect.task
        def cache_input(input: Any) -> None:
            r.cache_input(input)

        @prefect.task
        def store_chunk(input: Any) -> None:
            r.store_chunk(input)

        @prefect.task
        def finalize() -> None:
            r.finalize()

        with prefect.Flow("Pangeo-Forge") as flow:
            prepare()
            cache_input.map(list(r.iter_inputs()))
            store_chunk.map(list(r.iter_chunks()))
            finalize()

        return flow


    def execute_plan(self, plan: prefect.Flow, **kwargs):
        return plan.run(**kwargs)
