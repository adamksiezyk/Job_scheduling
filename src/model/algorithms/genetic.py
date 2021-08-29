from random import sample, choices, randint
from typing import TypeVar, Callable

from src.model.entities.job import Job

Creature = TypeVar("Creature")
Genome = list[Job]
Population = list[Genome]
FitnessFunc = Callable[[Genome], float]


class GeneticAlgorithm:
    def __init__(self, creatures: list[Creature], fitness: FitnessFunc):
        """
        @param creatures: list of jobs that form a genome
        @param fitness: a function that calculates the fitness weight of the given genome
        """
        self.creatures = creatures
        self.fitness = fitness

    def create_genome(self) -> Genome:
        """
        Creates a genome out of the creatures
        @return: genome
        """
        return sample(self.creatures, len(self.creatures))

    def create_population(self, amount: int) -> Population:
        """
        Creates a population
        @param amount: amount of genomes in the population
        @return: population
        """
        return [self.create_genome() for _ in range(amount)]

    def selection(self, population: Population, amount: int = 2) -> list[Genome]:
        """
        Returns the amount best genomes with the probability defined by the fitness function
        @param population: a population
        @param amount: amount of genomes to return
        @return: amount best genomes
        """
        return choices(population=population, weights=[self.fitness(genome) for genome in population], k=amount)

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
