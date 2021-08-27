import math
from random import sample

from src.model.entities.job import Job
from src.model.entities.resource import Resource
from src.model.entities.scheduler import Scheduler

Genome = list[Job]
Population = list[Genome]


class GeneticAlgorithm:
    def __init__(self, jobs: list[Job], resources: list[Resource]):
        """
        @param jobs: list of jobs that form a genome
        @param resources: list of available resources
        """
        self.jobs = jobs
        self.resources = resources

    def create_genome(self) -> Genome:
        """
        Creates a genome out of the creatures
        @return: genome
        """
        return sample(self.jobs, len(self.jobs))

    def create_population(self, amount: int) -> Population:
        """
        Creates a population
        @param amount: amount of genomes in the population
        @return: population
        """
        return [self.create_genome() for _ in range(amount)]

    def fitness(self, genome: Genome) -> float:
        """
        Returns the fitness value of a genome. The timestamp of the whole process duration or infinity if an scheduling
        error occurred
        @param genome: genome
        @return: fitness value of the provided genome
        """
        scheduler = Scheduler(resources=self.resources)
        try:
            for creature in genome:
                scheduler.schedule_job(creature)
        except ValueError:
            return math.inf
        return scheduler.calculate_queue_duration()
