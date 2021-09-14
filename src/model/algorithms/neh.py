import functools
from typing import TypeVar, Callable

import numpy as np

from src.model.algorithms.algorithm import SchedulingAlgorithm, Algorithm
from src.model.entities.job import Job
from src.model.entities.resource import Resource
from src.utils import lib

Element = TypeVar("Element")
FitnessFunc = Callable[[list[Element]], float]


class Neh(Algorithm):
    """
    NEH algorithm
    """

    def __init__(self, elements: list[Element], fitness: FitnessFunc):
        self.elements = elements
        self.fitness = fitness

    def optimize(self) -> list[Job]:
        """
        Returns the best solution
        @return: best solution
        """
        jobs = sorted(self.elements, key=lambda j: j.duration, reverse=True)
        return functools.reduce(self.__find_best_solution, jobs, [])

    def __find_best_solution(self, previous_solution: list[Element], element: Element) -> list[Element]:
        """
        Inserts the job to the queue and finds the best position for it
        @param previous_solution: list of jobs
        @param element: job to insert
        @return: best new queue
        """
        durations = map(lambda j: self.fitness(lib.insert_to_list(previous_solution, j, element)),
                        range(len(previous_solution) + 1))
        min_idx = int(np.argmin(durations))
        return lib.insert_to_list(previous_solution, min_idx, element)


class NehScheduler(SchedulingAlgorithm, Neh):
    """
    NEH scheduler
    """

    def __init__(self, jobs: list[Job], resources: list[Resource]):
        SchedulingAlgorithm.__init__(self, jobs=jobs, resources=resources)
        Neh.__init__(self, elements=jobs, fitness=self._calculate_queue_duration)
