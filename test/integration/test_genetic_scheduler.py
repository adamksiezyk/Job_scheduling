import unittest
from datetime import datetime, timedelta
from random import sample

from src.model.algorithms.genetic import SchedulingGeneticAlgorithm
from src.model.entities.job import ScheduledJob
from src.model.entities.project import Project
from src.model.entities.resource import Resource
from src.model.entities.scheduler import Scheduler


class TestGeneticScheduler(unittest.TestCase):
    def setUp(self):
        self.r1_m1 = Resource(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 14), machine_id="M1",
                              worker_amount=1)
        self.r2_m1 = Resource(start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 22), machine_id="M1",
                              worker_amount=2)
        self.r3_m1 = Resource(start_dt=datetime(2021, 4, 1, 22), end_dt=datetime(2021, 4, 2, 6), machine_id="M1",
                              worker_amount=3)
        self.r1_m2 = Resource(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 14), machine_id="M2",
                              worker_amount=3)
        self.r2_m2 = Resource(start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 22), machine_id="M2",
                              worker_amount=2)
        self.r3_m2 = Resource(start_dt=datetime(2021, 4, 1, 22), end_dt=datetime(2021, 4, 2, 6), machine_id="M2",
                              worker_amount=2)
        self.resources = [self.r1_m1, self.r2_m1, self.r3_m1, self.r1_m2, self.r2_m2, self.r3_m2]

        self.project = Project(start_dt=datetime(2021, 3, 28, 6), expiration_dt=datetime(2021, 4, 10), id="P1")
        self.j1 = ScheduledJob(start_dt=datetime(2021, 3, 28, 6), end_dt=datetime(2021, 3, 28, 14),
                               duration=timedelta(hours=8), delay='0d', machine_id="M1", project=self.project)
        self.j2 = ScheduledJob(start_dt=datetime(2021, 3, 28, 6), end_dt=datetime(2021, 3, 28, 14),
                               duration=timedelta(hours=8), delay='0d', machine_id="M2", project=self.project)
        self.j3 = ScheduledJob(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 14),
                               duration=timedelta(hours=8), delay='0d', machine_id="M1", project=self.project)
        self.j4 = ScheduledJob(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 10),
                               duration=timedelta(hours=4), delay='0d', machine_id="M2", project=self.project)
        self.jobs = [self.j1, self.j2, self.j3, self.j4]
        self.creatures = list(range(4))

        self.genetic = SchedulingGeneticAlgorithm(jobs=self.jobs, resources=self.resources)

    def test_create_genome(self):
        self.assertCountEqual(self.creatures, self.genetic.create_genome())

    def test_create_population(self):
        [self.assertCountEqual(self.creatures, genome) for genome in self.genetic.create_population(10)]

    def test_fitness(self):
        self.assertEqual(1 / datetime(2021, 4, 1, 21, 20).timestamp(), self.genetic.fitness([2, 0, 1, 3]))

    def test_selection(self):
        population = [sample(self.creatures, len(self.creatures)) for _ in range(10)]
        [self.assertCountEqual(self.creatures, genome) for genome in self.genetic.selection(population)]

    def test_crossover(self):
        a = self.creatures
        b = sample(self.creatures, len(self.creatures))
        [self.assertCountEqual(self.creatures, genome) for genome in self.genetic.crossover(a, b)]

    def test_mutation(self):
        self.assertCountEqual(self.creatures, self.genetic.mutation(self.creatures, 2, 0.8))

    def test_evolution(self):
        solution = self.genetic.optimize(5, 10, 2, 0.6)
        scheduler = Scheduler(self.resources)
        [scheduler.schedule_job(self.jobs[i]) for i in solution]
        print(f"Optimal solution: {solution}, duration {scheduler.calculate_queue_duration()}")
        self.assertCountEqual(self.creatures, solution)
