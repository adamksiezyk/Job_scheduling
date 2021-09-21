from abc import abstractmethod, ABC
from random import sample, choices, random, randint
from typing import TypeVar, Callable

from src.model.algorithms.algorithm import Algorithm, SchedulingAlgorithm
from src.model.entities.job import Job
from src.model.entities.resource import Resource
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
    def __init__(self, jobs: list[Job], resources: dict[str, list[Resource]]):
        SchedulingAlgorithm.__init__(self, jobs=jobs, resources=resources)
        Genetic.__init__(self, creatures=list(range(len(jobs))))

    def create_genome(self) -> Genome:
        """
        Creates a genome out of the creatures
        @return: genome
        """
        return sample(self.CREATURES, len(self.CREATURES))

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
        length = len(a)
        if length != len(b):
            raise ValueError("Both genomes have to be equal lengths")

        p = randint(1, length - 1)
        leftover_a = [elem for elem in a[p:] if elem not in b[p:]]
        leftover_b = [elem for elem in b[p:] if elem not in a[p:]]
        child_a = [elem if elem not in b[p:] else leftover_a.pop(0) for elem in a[:p]] + b[p:]
        child_b = [elem if elem not in a[p:] else leftover_b.pop(0) for elem in b[:p]] + a[p:]
        return child_a, child_b

    def mutation(self, genome: Genome, amount: int, probability: float) -> Genome:
        """
        Mutates the amount creatures of a genome with the given probability
        @param genome: a genome
        @param amount: amount of creatures to mutate
        @param probability: probability of the mutation of a single creature
        @return: genome with mutated creatures
        """
        length = len(genome)
        if amount > length:
            raise ValueError("Amount of jobs to modify is greater then amount of jobs in genome")

        mutated_genome = [*genome]
        for idx, new_idx in zip(sample(range(length), amount), sample(range(length), amount)):
            if random() > probability:
                continue
            mutated_genome[idx], mutated_genome[new_idx] = mutated_genome[new_idx], mutated_genome[idx]
        return mutated_genome

    def fitness(self, genome: Genome) -> float:
        """
        A fitness function that returns the fitness weight of the give genome
        @param genome: a genome
        @return: a fitness weight of the given genome
        """
        queue = [self.JOBS[i] for i in genome]
        return SchedulingAlgorithm.fitness(self, queue)
