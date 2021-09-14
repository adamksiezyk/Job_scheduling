from abc import ABC
from itertools import permutations
from typing import TypeVar, Callable

import numpy as np

from src.model.algorithms.algorithm import SchedulingAlgorithm, Algorithm
from src.model.entities.job import Job
from src.model.entities.resource import Resource

Element = TypeVar("Element")
FitnessFunc = Callable[[list[Element]], float]


class BruteForce(Algorithm, ABC):
    """
    Brute force algorithm
    """

    def __init__(self, elements: list[Element], fitness: FitnessFunc):
        self.elements = elements
        self.fitness = fitness

    def optimize(self) -> list[Element]:
        """
        Returns the best solution
        @return: best solution
        """
        possible_solutions = list(permutations(self.elements))
        durations = map(self.fitness, possible_solutions)
        min_idx = int(np.argmax(durations))
        return possible_solutions[min_idx]


class BruteForceScheduler(SchedulingAlgorithm, BruteForce):
    """
    Brute force scheduler
    """

    def __init__(self, jobs: list[Job], resources: list[Resource]):
        SchedulingAlgorithm.__init__(self, jobs=jobs, resources=resources)
        BruteForce.__init__(self, elements=jobs)
