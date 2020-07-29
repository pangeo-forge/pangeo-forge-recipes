from abc import ABC, abstractmethod
from typing import List

from prefect import Flow


class AbstractPipeline(ABC):
    name = "AbstractPipeline"

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
