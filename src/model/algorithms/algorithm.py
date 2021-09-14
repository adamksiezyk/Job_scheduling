import math
from abc import ABC, abstractmethod
from typing import TypeVar

from src.model.entities.job import Job
from src.model.entities.resource import Resource
from src.model.entities.scheduler import Scheduler

Solution = TypeVar("Solution")


class Algorithm(ABC):
    @abstractmethod
    def optimize(self, *args, **kwargs) -> Solution:
        """
        Finds the best solution for a problem
        @param args:
        @param kwargs:
        @return: solution to the problem
        """


class SchedulingAlgorithm(Algorithm, ABC):
    """
    Base class for a Scheduling Algorithm
    """

    def __init__(self, jobs: list[Job], resources: list[Resource]):
        """
        @param jobs: list of jobs to schedule
        @param resources: list of available resources
        """
        self.jobs = [*jobs]
        self.resources = [*resources]

    def _calculate_queue_duration(self, queue: list[Job]) -> float:
        """
        Returns the duration of the queue
        @param queue: list of jobs
        @return: duration of the queue
        """
        scheduler = Scheduler([*self.resources])
        try:
            for job in queue:
                scheduler.schedule_job(job)
        except ValueError:
            return math.inf
        return scheduler.calculate_queue_duration()
