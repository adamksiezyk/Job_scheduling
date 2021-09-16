from abc import ABC, abstractmethod
from typing import TypeVar

from src.model.entities.job import Job
from src.model.entities.resource import Resource
from src.model.entities.scheduler import Scheduler

Solution = TypeVar("Solution")


class Algorithm(ABC):
    @abstractmethod
    def fitness(self, sample_solution) -> float:
        """
        A fitness function that returns the fitness weight of the give sample solution
        @param sample_solution: a sample solution
        @return: a fitness weight of the given sample solution
        """

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

    def __init__(self, jobs: list[Job], resources: dict[str, list[Resource]]):
        """
        @param jobs: list of jobs to schedule
        @param resources: list of available resources
        """
        self.JOBS = jobs
        self.RESOURCES = resources

    def fitness(self, queue: list[Job]) -> float:
        """
        Returns the duration of the queue
        @param queue: list of jobs
        @return: duration of the queue
        """
        scheduler = Scheduler(self.RESOURCES)
        try:
            for job in queue:
                scheduler.schedule_job(job)
        except ValueError:
            return 0
        return 1 / scheduler.calculate_queue_duration()
