"""
Base class for most pipelines.

Design
------

To the extent possible, we want recipe maintainers to focus on the code
needed to express the data transformations. In particular, we don't
want them to worry about things like the execution environment and how
their code is loaded into it.

The Flow
========

We use prefect_ to express ETL pipelines.


Implementation
--------------

Most recipes will inherit from :class:`AbstractPipeline`. This base class
provides:

1. Abstract methods and properties required to run a pipeline
2. Default Prefect Storage and Environment implementations.

.. _prefect: https://docs.prefect.io/
"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from prefect import Flow
from prefect.environments import DaskKubernetesEnvironment, Environment
from prefect.environments.storage import Storage
from prefect.environments.storage.github import GitHub

HERE = Path(__file__).parent.absolute()


class AbstractPipeline(ABC):
    name = "AbstractPipeline"
    repo = None  # defaults to pangeo-forge/{feedstock}
    path = "recipe/pipeline.py"

    @property
    @abstractmethod
    def repo(self):
        """The GitHub repository containing the pipeline definition."""
        # TODO: This changes when we merge it. From staged to feedstock?

    @property
    @abstractmethod
    def sources(self) -> List[str]:
        """A list of source URLs containing the original data."""
        pass

    @property
    @abstractmethod
    def targets(self) -> List[str]:
        """A list of target URLs where the transformed data is written."""
        pass

    @abstractmethod
    def flow(self) -> Flow:
        """The """
        pass

    @property
    def environment(self) -> Environment:
        """
        The pipeline runtime environment.

        Returns
        -------
        prefect.environments.Environment
            An instance of a Prefect Environment. By default
            a :class:`prefect.environments.DaskKubernetesEnvironment`
            is used.
        """
        scheduler_spec_file = str(HERE / "job.yaml")
        worker_spec_file = str(HERE / "worker_pod.yaml")

        environment = DaskKubernetesEnvironment(
            min_workers=1,
            max_workers=30,
            scheduler_spec_file=scheduler_spec_file,
            worker_spec_file=worker_spec_file,
            metadata=dict(image="pangeoforge/default-image"),
        )
        return environment

    @property
    def storage(self) -> Storage:
        """
        The pipeline storage.

        Returns
        -------
        prefect.environments.storage.Storage
            By default a :class:`prefect.environments.storage.github.GitHub`
            environment is used with ``self.repo`` as the repository
            and ``self.path`` as the path.
        """
        return GitHub(self.repo, path=self.path)
