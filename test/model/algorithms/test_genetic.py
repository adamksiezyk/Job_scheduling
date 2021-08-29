from datetime import datetime, timedelta
from random import seed
from unittest import TestCase

from src.model.algorithms.genetic import GeneticAlgorithm
from src.model.entities.job import Job
from src.model.entities.project import Project
from src.model.entities.resource import Resource
from src.model.entities.scheduler import Scheduler


class TestGeneticAlgorithm(TestCase):
    def setUp(self):
        r1 = Resource(start_dt=datetime(2021, 3, 4, 6), end_dt=datetime(2021, 3, 4, 14), machine_id="M1",
                      worker_amount=1)
        r2 = Resource(start_dt=datetime(2021, 3, 4, 6), end_dt=datetime(2021, 3, 4, 14), machine_id="M2",
                      worker_amount=2)
        r3 = Resource(start_dt=datetime(2021, 3, 4, 14), end_dt=datetime(2021, 3, 4, 22), machine_id="M1",
                      worker_amount=2)
        r4 = Resource(start_dt=datetime(2021, 3, 4, 14), end_dt=datetime(2021, 3, 4, 22), machine_id="M2",
                      worker_amount=2)
        r5 = Resource(start_dt=datetime(2021, 3, 4, 22), end_dt=datetime(2021, 3, 5, 6), machine_id="M1",
                      worker_amount=2)
        r6 = Resource(start_dt=datetime(2021, 3, 4, 22), end_dt=datetime(2021, 3, 5, 6), machine_id="M2",
                      worker_amount=2)
        self.resources = [r1, r2, r3, r4, r5, r6]
        p = Project(id="P1", start_dt=datetime(2021, 3, 1), expiration_dt=datetime(2021, 3, 31))
        j1 = Job(project=p, duration=timedelta(hours=4), delay="0d", machine_id="M1")
        j2 = Job(project=p, duration=timedelta(hours=15), delay="0d", machine_id="M2")
        j3 = Job(project=p, duration=timedelta(hours=8), delay="0d", machine_id="M1")
        self.jobs = [j1, j2, j3]
        fitness = Scheduler.create_fitness_function(self.resources)
        self.genetic_algorithm = GeneticAlgorithm(self.jobs, fitness)

    def test_create_genome(self):
        self.assertCountEqual(self.jobs, self.genetic_algorithm.create_genome())

    def test_create_population(self):
        [self.assertCountEqual(self.jobs, genome) for genome in self.genetic_algorithm.create_population(10)]

    def test_fitness(self):
        self.assertEqual(datetime(2021, 3, 4, 21, 30).timestamp(), self.genetic_algorithm.fitness(self.jobs))

    def test_selection(self):
        seed(2000)
        population = [self.jobs, self.jobs[-1::-1], [self.jobs[1], self.jobs[-1], self.jobs[0]]]
        self.assertEqual([population[1], population[2]], self.genetic_algorithm.selection(population, 2))

    def test_crossover(self):
        a = self.jobs
        b = self.jobs[-1::-1]
        new_a, new_b = self.genetic_algorithm.crossover(a, b)
        self.assertCountEqual(self.jobs, new_a)
        self.assertCountEqual(self.jobs, new_b)
