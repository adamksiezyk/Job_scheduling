from abc import ABC, abstractmethod
from typing import TypeVar

Solution = TypeVar("Solution")


class Algorithm(ABC):
    @abstractmethod
    def optimize(self, *args, **kwargs) -> tuple[Solution]:
        """
        Finds the best solution for a problem
        @param args:
        @param kwargs:
        @return:
        """
