from datetime import datetime, timedelta
from unittest import TestCase

from src.model.entities.job import ScheduledJob, Job
from src.model.entities.project import Project
from src.model.entities.resource import Resource
from src.model.entities.scheduler import Scheduler


class TestScheduler(TestCase):
    def setUp(self) -> None:
        self.r1_m1 = Resource(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 14), machine_id="M1",
                              worker_amount=2)
        self.r2_m1 = Resource(start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 22), machine_id="M1",
                              worker_amount=2)
        self.r3_m1 = Resource(start_dt=datetime(2021, 4, 1, 22), end_dt=datetime(2021, 4, 2, 6), machine_id="M1",
                              worker_amount=2)
        self.r1_m2 = Resource(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 14), machine_id="M2",
                              worker_amount=2)
        self.r2_m2 = Resource(start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 22), machine_id="M2",
                              worker_amount=2)
        self.r3_m2 = Resource(start_dt=datetime(2021, 4, 1, 22), end_dt=datetime(2021, 4, 2, 6), machine_id="M2",
                              worker_amount=2)
        self.resources = [self.r1_m1, self.r2_m1, self.r3_m1, self.r1_m2, self.r2_m2, self.r3_m2]

        self.empty_scheduler = Scheduler()

        self.project = Project(start_dt=datetime(2021, 3, 28, 6), expiration_dt=datetime(2021, 4, 10), id="P1")
        j1 = ScheduledJob(start_dt=datetime(2021, 3, 28, 6), end_dt=datetime(2021, 3, 28, 14),
                          duration=timedelta(hours=8), delay='0d', machine_id="M1", project=self.project)
        j2 = ScheduledJob(start_dt=datetime(2021, 3, 28, 6), end_dt=datetime(2021, 3, 28, 14),
                          duration=timedelta(hours=8), delay='1d', machine_id="M2", project=self.project)
        j3 = ScheduledJob(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 14),
                          duration=timedelta(hours=8), delay='0d', machine_id="M1", project=self.project)
        j4 = ScheduledJob(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 10),
                          duration=timedelta(hours=4), delay='0d', machine_id="M2", project=self.project)
        self.queue = [j1, j2, j3, j4]
        self.scheduler = Scheduler(queue=[*self.queue], resources=[*self.resources])

    def test_schedule_job(self):
        j1 = Job(duration=timedelta(hours=6), delay="1d", machine_id="M0", project=self.project)
        j2 = Job(duration=timedelta(hours=6), delay="0d", machine_id="M1", project=self.project)
        s2 = ScheduledJob(duration=j2.duration, delay=j2.delay, machine_id=j2.machine_id, project=j2.project,
                          start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 17))

        j3 = Job(duration=timedelta(hours=20), delay="1d", machine_id="M2", project=self.project)
        s3_1 = ScheduledJob(duration=timedelta(hours=8), delay="0d", machine_id=j3.machine_id, project=j3.project,
                            start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 22))
        s3_2 = ScheduledJob(duration=timedelta(hours=12), delay=j3.delay, machine_id=j3.machine_id, project=j3.project,
                            start_dt=datetime(2021, 4, 1, 22), end_dt=datetime(2021, 4, 2, 4))
        # Test no resources available
        self.assertRaises(ValueError, self.empty_scheduler.schedule_job, j1)
        # Test machine not found
        self.assertRaises(ValueError, self.scheduler.schedule_job, j1)
        # Test one resource job schedule
        self.scheduler.schedule_job(j2)
        self.assertEqual(self.queue + [s2], self.scheduler.queue)
        # Test multiple resource job schedule
        self.scheduler.schedule_job(j3)
        self.assertEqual(self.queue + [s2, s3_1, s3_2], self.scheduler.queue)

    def test_find_available_resources_for_machine(self):
        self.assertEqual([], self.empty_scheduler.find_available_resources_for_machine("M1"))
        self.assertEqual([], self.scheduler.find_available_resources_for_machine("M0"))
        self.assertEqual([self.r1_m1, self.r2_m1, self.r3_m1],
                         self.scheduler.find_available_resources_for_machine("M1"))
        self.assertEqual([self.r1_m2, self.r2_m2, self.r3_m2],
                         self.scheduler.find_available_resources_for_machine("M2"))

    def test_find_available_resources_for_machine_and_dt(self):
        self.assertEqual([], self.empty_scheduler.find_available_resources_for_machine_and_dt(
            "M1", datetime(2021, 4, 1, 8)))
        self.assertEqual([], self.scheduler.find_available_resources_for_machine_and_dt("M0", datetime(2021, 4, 1, 8)))
        self.assertEqual([], self.scheduler.find_available_resources_for_machine_and_dt("M1", datetime(2021, 4, 3, 8)))
        self.assertEqual([self.r2_m1, self.r3_m1],
                         self.scheduler.find_available_resources_for_machine_and_dt("M1", datetime(2021, 4, 1, 15)))
        self.assertEqual([self.r3_m2],
                         self.scheduler.find_available_resources_for_machine_and_dt("M2", datetime(2021, 4, 2, 3)))

    def test_find_earliest_resource(self):
        self.assertEqual(None, self.empty_scheduler.find_earliest_resource("M1", datetime(2021, 3, 28, 8)))
        self.assertEqual(None, self.scheduler.find_earliest_resource("M0", datetime(2021, 3, 28, 8)))
        self.assertEqual(None, self.scheduler.find_earliest_resource("M1", datetime(2021, 4, 20, 8)))

    def test_find_scheduled_jobs(self):
        self.assertEqual([], self.empty_scheduler.find_scheduled_jobs(self.project.id))
        self.assertEqual([], self.scheduler.find_scheduled_jobs("P0"))
        self.assertEqual(self.queue, self.scheduler.find_scheduled_jobs(self.project.id))

    def test_find_last_scheduled_job(self):
        self.assertEqual(None, self.empty_scheduler.find_last_scheduled_job(self.project.id))
        self.assertEqual(None, self.scheduler.find_last_scheduled_job("P0"))
        self.assertEqual(datetime(2021, 4, 1, 14), self.scheduler.find_last_scheduled_job(self.project.id).end_dt)
