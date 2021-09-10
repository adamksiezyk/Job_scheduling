from random import seed, choices, randint, random, sample
from unittest import TestCase

from src.model.algorithms.genetic import Genome, Population, GeneticAlgorithm


class BasicProblem(GeneticAlgorithm):
    def create_genome(self) -> Genome:
        return randint(-20, 20)

    def create_population(self, amount: int) -> Population:
        return [self.create_genome() for _ in range(amount)]

    def selection(self, population: Population, amount: int = 2) -> list[Genome]:
        return choices(population=population, weights=[self.fitness(genome) for genome in population], k=amount)

    def crossover(self, a: Genome, b: Genome) -> tuple[Genome, Genome]:
        return ((a / 2) + b) / 2, ((b / 2) + a) / 2

    def mutation(self, genome: Genome, amount: int, probability: float) -> Genome:
        return sum(genome * sample([-1, 1], 1)[0] * 3 / 4 + 2 for _ in range(amount) if random() < probability)

    def fitness(self, genome: Genome) -> float:
        return (genome - 2) ** 2


class TestGeneticAlgorithm(TestCase):
    def setUp(self):
        self.basic_problem = BasicProblem(0)

    def test_create_genome(self):
        self.assertIsInstance(self.basic_problem.create_genome(), int)

    def test_create_population(self):
        population = self.basic_problem.create_population(10)
        self.assertEqual(10, len(population))
        [self.assertIsInstance(genome, int) for genome in population]

    def test_fitness(self):
        self.assertAlmostEqual(0.25, self.basic_problem.fitness(2.5))

    def test_selection(self):
        seed(2000)
        population = [self.basic_problem.creatures, self.basic_problem.creatures * 3,
                      self.basic_problem.creatures / 2]
        self.assertTrue(all([genome in population for genome in self.basic_problem.selection(population)]))

    def test_crossover(self):
        seed(2000)
        a = self.basic_problem.creatures
        b = self.basic_problem.creatures * 2 - 3
        new_a, new_b = self.basic_problem.crossover(a, b)
        self.assertIsInstance(new_a, float)
        self.assertIsInstance(new_b, float)

    def test_mutation(self):
        seed(2000)
        self.assertIsInstance(self.basic_problem.mutation(10, 3, 0.7), float)

    def test_evolution(self):
        seed(2000)
        solution = self.basic_problem.optimize(5, 10, 1, 0.5)
        print(f"min{{(x-2)^2}} = {solution}")
        self.assertIsInstance(solution, float)
