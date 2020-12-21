from typing import Any

import prefect


class PrefectExecutor:
    def prepare_plan(self, r) -> prefect.Flow:

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
            prep_task = prepare()
            cache_task = cache_input.map(list(r.iter_inputs()))
            cache_task.set_dependencies(upstream_tasks=[prep_task])
            store_task = store_chunk.map(list(r.iter_chunks()))
            store_task.set_dependencies(upstream_tasks=[cache_task])
            finalize_task = finalize()
            finalize_task.set_dependencies(upstream_tasks=[store_task])

        return flow

    def execute_plan(self, plan: prefect.Flow, **kwargs):
        return plan.run(**kwargs)
