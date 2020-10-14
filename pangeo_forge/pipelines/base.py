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
    def sources(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def targets(self) -> List[str]:
        pass

    @abstractmethod
    def flow(self) -> Flow:
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
            metadata=dict(image="tomaugspurger/pangeo-forge"),
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
