import functools
from abc import ABC, abstractmethod
from typing import TypeVar, Callable

import numpy as np

from src.model.algorithms.algorithm import SchedulingAlgorithm, Algorithm
from src.model.db.excel.fetch import fetch_jobs_dict_from_list
from src.model.entities.job import Job
from src.model.entities.resource import Resource
from src.utils import lib

Element = TypeVar("Element")
FitnessFunc = Callable[[list[Element]], float]


class Neh(Algorithm, ABC):
    """
    NEH algorithm
    """

    def __init__(self, elements: list[Element]):
        self.ELEMENTS = elements

    def optimize(self) -> list[Job]:
        """
        Returns the best solution
        @return: best solution
        """
        jobs = self.sort_function(self.ELEMENTS)
        return functools.reduce(self.__find_best_solution, jobs, [])

    @abstractmethod
    def sort_function(self, elements: list[Element]) -> list[Element]:
        """
        Sorts the list of elements from shortest duration to longest duration
        @param elements: list of Elements
        @return: sorted list of elements
        """

    def __find_best_solution(self, previous_solution: list[Element], element: Element) -> list[Element]:
        """
        Inserts the job to the queue and finds the best position for it
        @param previous_solution: list of jobs
        @param element: job to insert
        @return: best new queue
        @raise RuntimeError: when no solutions are valid
        """
        durations = [self.fitness(lib.insert_to_list(previous_solution, j, element))
                     for j in range(len(previous_solution) + 1)]
        if sum(durations) == 0:
            raise RuntimeError("No solutions found")
        min_idx = int(np.argmax(durations))
        return lib.insert_to_list(previous_solution, min_idx, element)


class NehScheduler(SchedulingAlgorithm, Neh):
    """
    NEH scheduler
    """

    def __init__(self, jobs: list[Job], resources: dict[str, list[Resource]]):
        SchedulingAlgorithm.__init__(self, jobs=jobs, resources=resources)
        Neh.__init__(self, elements=jobs)

    def sort_function(self, elements: list[Element]) -> list[Element]:
        """
        Returns a sorted elements list
        @param elements: elements list
        @return: sorted elements list
        """
        jobs_dict = fetch_jobs_dict_from_list(elements)
        jobs_grouped_by_project = [[job for job in jobs] for jobs in jobs_dict.values()]
        return self.sort_jobs(jobs_grouped_by_project)

    @staticmethod
    def sort_jobs(jobs_grouped_by_project: list[list[Job]]) -> list[Job]:
        return functools.reduce(NehScheduler.sort_jobs_in_project, jobs_grouped_by_project[1:],
                                jobs_grouped_by_project[0])

    @staticmethod
    def sort_jobs_in_project(jobs, project: list[Job]) -> list[Job]:
        prev_idx = 0
        for job in project:
            idx = NehScheduler.find_insertion_index_in_sorted_list(jobs, job)
            new_idx = prev_idx + 1 if idx <= prev_idx else idx
            jobs.insert(new_idx, job)
            prev_idx = new_idx
        return jobs

    @staticmethod
    def find_insertion_index_in_sorted_list(sorted_jobs: list[Job], job: Job) -> int:
        for (i, j) in enumerate(sorted_jobs):
            if j.duration > job.duration:
                return i
        return len(sorted_jobs)
