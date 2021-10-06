from abc import abstractmethod, ABC
from random import sample, choices, random, randint, shuffle
from typing import TypeVar, Callable

from src.model.algorithms.algorithm import Algorithm, SchedulingAlgorithm
from src.model.db.excel.fetch import fetch_jobs_dict_from_list
from src.model.entities.job import Job
from src.model.entities.resource import Resources
from src.utils import lib

Genome = TypeVar("Genome")
Population = list[Genome]
FitnessFunc = Callable[[Genome], float]


class Genetic(Algorithm, ABC):
    def __init__(self, creatures: Genome):
        """
        @param creatures: list of creatures that form a genome
        """
        self.CREATURES = creatures

    @abstractmethod
    def create_genome(self) -> Genome:
        """
        Creates a genome out of the creatures
        @return: genome
        """

    @abstractmethod
    def create_population(self, amount: int) -> Population:
        """
        Creates a population
        @param amount: amount of genomes in the population
        @return: population
        """

    @abstractmethod
    def selection(self, population: Population, weights: list[float], amount: int = 2) -> list[Genome]:
        """
        Returns the amount best genomes with the probability defined by the fitness function
        @param population: a population
        @param weights: a list of weights of the genomes in a population
        @param amount: amount of genomes to return
        @return: amount best genomes
        """

    @abstractmethod
    def crossover(self, a: Genome, b: Genome) -> tuple[Genome, Genome]:
        """
        Returns the crossover children of two parent genomes
        @param a: parent A
        @param b: parent B
        @return: crossover children of the parent genomes
        """

    @abstractmethod
    def mutation(self, genome: Genome, amount: int, probability: float) -> Genome:
        """
        Mutates the amount creatures of a genome with the given probability
        @param genome: a genome
        @param amount: amount of creatures to mutate
        @param probability: probability of the mutation of a single creature
        @return: genome with mutated creatures
        """

    @abstractmethod
    def fitness(self, genome: Genome) -> float:
        """
        A fitness function that returns the fitness weight of the give genome
        @param genome: a genome
        @return: a fitness weight of the given genome
        """

    def optimize(self, population_size: int, generation_limit: int, mutation_amount: int,
                 mutation_probability: float) -> Genome:
        """
        Finds the optimal solution of the given problem
        @param population_size: the number of genomes in a population
        @param generation_limit: the maximum generations number
        @param mutation_amount: the amount of creatures to mutate in a genome
        @param mutation_probability: the probability of mutation of a single genome
        @return: the best population and the generation number
        """
        population = self.create_population(population_size)
        for i in range(generation_limit):
            fitness_values = [self.fitness(genome) for genome in population]
            sort = sorted(zip(fitness_values, population), reverse=True)
            population = list(lib.get_column(1)(sort))
            fitness_values = list(lib.get_column(0)(sort))

            # if self.fitness(population[0]) < 5:
            #     break
            print(f"Generation: {i},\tfitness: {fitness_values[0]}")

            next_generation = population[:2]
            for j in range(int(population_size / 2) - 1):
                parents = self.selection(population, fitness_values)
                offspring_a, offspring_b = self.crossover(*parents)
                offspring_a = self.mutation(offspring_a, mutation_amount, mutation_probability)
                offspring_b = self.mutation(offspring_b, mutation_amount, mutation_probability)
                next_generation.append(offspring_a)
                next_generation.append(offspring_b)

            population = next_generation

        return max(population, key=self.fitness)


class GeneticScheduler(SchedulingAlgorithm, Genetic):
    def __init__(self, jobs: list[Job], resources: Resources):
        SchedulingAlgorithm.__init__(self, jobs=jobs, resources=resources)
        Genetic.__init__(self, creatures=list(range(len(jobs))))

    def create_genome(self) -> Genome:
        """
        Creates a genome out of the creatures
        @return: genome
        """
        creatures_dict = fetch_jobs_dict_from_list(self.JOBS)
        indices = list(range(sum(len(c) for c in creatures_dict.values())))
        indices_grouped_by_project = [[indices.pop(0) for _ in range(len(c))] for c in creatures_dict.values()]
        shuffle(indices_grouped_by_project)  # Shuffle projects
        return self.shuffle_jobs(indices_grouped_by_project)

    @staticmethod
    def shuffle_jobs(jobs_grouped_by_project: list[list]) -> list:
        shuffled_jobs = []
        for jobs in jobs_grouped_by_project:
            i = 0
            for job in jobs:
                n = randint(i, len(shuffled_jobs))
                shuffled_jobs.insert(n, job)
                i = n + 1
        return shuffled_jobs

    def create_population(self, amount: int) -> Population:
        """
        Creates a population
        @param amount: amount of genomes in the population
        @return: population
        """
        return [self.create_genome() for _ in range(amount)]

    def selection(self, population: Population, weights: list[float], amount: int = 2) -> list[Genome]:
        """
        Returns the amount best genomes with the probability defined by the fitness function
        @param population: a population
        @param weights: a list of weights of the genomes in the population
        @param amount: amount of genomes to return
        @return: amount best genomes
        """
        return choices(population=population, weights=weights, k=amount)

    def crossover(self, a: Genome, b: Genome) -> tuple[Genome, Genome]:
        """
        Returns the crossover children of two parent genomes
        @param a: parent A
        @param b: parent B
        @return: crossover children of the parent genomes
        """
        parent_a = [(c, self.JOBS[c]) for c in a]
        parent_b = [(c, self.JOBS[c]) for c in b]
        subset_len = randint(1, len(self.JOBS))
        subset_projects = {job.project for job in sample(self.JOBS, subset_len)}
        return self._pmx(parent_a, parent_b, subset_projects), self._pmx(parent_b, parent_a, subset_projects)

    @staticmethod
    def _pmx(a, b, projects):
        """
        Partial Mapper Crossover
        @param a: parent a
        @param b: parent b
        @param projects: list of projects to crossover
        @return: child
        """
        subset = [c if job.project in projects else None for c, job in a]
        proto = [c for c, job in b if c not in subset]
        return [s if s is not None else proto.pop(0) for s in subset]

    def mutation(self, genome: Genome, amount: int, probability: float) -> Genome:
        """
        Mutates the amount creatures of a genome with the given probability
        @param genome: a genome
        @param amount: amount of creatures to mutate
        @param probability: probability of the mutation of a single creature
        @return: genome with mutated creatures
        """
        _genome = [self.JOBS[c] for c in genome]
        mutated_genome = [*genome]
        old_projects = {job.project for job in sample(self.JOBS, amount)}
        new_projects = {job.project for job in sample(self.JOBS, amount)}
        old_jobs_grouped_by_project = [[i for i, job in enumerate(_genome) if job.project == project]
                                       for project in old_projects]
        new_jobs_grouped_by_project = [[i for i, job in enumerate(_genome) if job.project == project]
                                       for project in new_projects]
        for old_jobs, new_jobs in zip(old_jobs_grouped_by_project, new_jobs_grouped_by_project):
            if random() > probability:
                continue
            for old_i, new_i in zip(old_jobs, new_jobs):
                mutated_genome[old_i], mutated_genome[new_i] = mutated_genome[new_i], mutated_genome[old_i]
        return mutated_genome

    def fitness(self, genome: Genome) -> float:
        """
        A fitness function that returns the fitness weight of the give genome
        @param genome: a genome
        @return: a fitness weight of the given genome
        """
        queue = [self.JOBS[i] for i in genome]
        return SchedulingAlgorithm.fitness(self, queue)
