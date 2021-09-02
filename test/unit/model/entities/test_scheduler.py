from dataclasses import replace
from datetime import datetime, timedelta
from unittest import TestCase

from src.model.entities.job import ScheduledJob, Job
from src.model.entities.project import Project
from src.model.entities.resource import Resource
from src.model.entities.scheduler import Scheduler


class TestScheduler(TestCase):
    def setUp(self) -> None:
        self.empty_scheduler = Scheduler([])

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
        self.resources_init = [self.r1_m1, self.r2_m1, self.r3_m1, self.r1_m2, self.r2_m2, self.r3_m2]

        self.project = Project(start_dt=datetime(2021, 3, 28, 6), expiration_dt=datetime(2021, 4, 10), id="P1")
        self.j1 = ScheduledJob(start_dt=datetime(2021, 3, 28, 6), end_dt=datetime(2021, 3, 28, 14),
                               duration=timedelta(hours=8), delay='0d', machine_id="M1", project=self.project)
        self.j2 = ScheduledJob(start_dt=datetime(2021, 3, 28, 6), end_dt=datetime(2021, 3, 28, 14),
                               duration=timedelta(hours=8), delay='1d', machine_id="M2", project=self.project)
        self.j3 = ScheduledJob(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 14),
                               duration=timedelta(hours=8), delay='0d', machine_id="M1", project=self.project)
        self.j4 = ScheduledJob(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 10),
                               duration=timedelta(hours=4), delay='0d', machine_id="M2", project=self.project)
        self.queue_init = [self.j1, self.j2, self.j3, self.j4]

        self.scheduler = Scheduler(resources=self.resources_init)
        self.scheduler.queue = [*self.queue_init]

    def test_calculate_queue_duration(self):
        self.assertEqual(datetime(2021, 4, 1, 14).timestamp(), self.scheduler.calculate_queue_duration())

    def test_schedule_job_no_resources(self):
        j1 = Job(duration=timedelta(hours=6), delay="1d", machine_id="M0", project=self.project)
        j2 = Job(duration=timedelta(hours=6), delay="0d", machine_id="M1", project=self.project)
        self.assertRaises(ValueError, self.empty_scheduler.schedule_job, j2)
        self.assertRaises(ValueError, self.scheduler.schedule_job, j1)

    def test_schedule_job_one_resource_is_enough(self):
        j = Job(duration=timedelta(hours=6), delay="0d", machine_id="M1", project=self.project)
        s = ScheduledJob(duration=j.duration, delay=j.delay, machine_id=j.machine_id, project=j.project,
                         start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 17))
        r = replace(self.r2_m1, start_dt=s.end_dt)
        self.resources_init.remove(self.r2_m1)
        self.resources_init.append(r)
        self.queue_init.append(s)

        self.scheduler.schedule_job(j)
        self.assertEqual(self.resources_init, self.scheduler.resources)
        self.assertEqual(self.queue_init, self.scheduler.queue)

    def test_schedule_job_one_resource_is_not_enough(self):
        j = Job(duration=timedelta(hours=20), delay="1d", machine_id="M2", project=self.project)
        s1 = ScheduledJob(duration=timedelta(hours=16), delay="0d", machine_id=j.machine_id, project=j.project,
                          start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 22))
        s2 = ScheduledJob(duration=timedelta(hours=4), delay=j.delay, machine_id=j.machine_id, project=j.project,
                          start_dt=datetime(2021, 4, 1, 22), end_dt=datetime(2021, 4, 2, 0))
        r = replace(self.r3_m2, start_dt=s2.end_dt)
        self.resources_init.remove(self.r2_m2)
        self.resources_init.remove(self.r3_m2)
        self.resources_init.append(r)
        self.queue_init.append(s1)
        self.queue_init.append(s2)

        self.scheduler.schedule_job(j)
        self.assertEqual(self.resources_init, self.scheduler.resources)
        self.assertEqual(self.queue_init, self.scheduler.queue)

    def test_schedule_job_before_project_start(self):
        p = Project(start_dt=datetime(2021, 4, 2), expiration_dt=datetime(2021, 4, 10), id="P1")
        j = Job(duration=timedelta(hours=10), delay="1d", machine_id="M2", project=p)
        self.scheduler.schedule_job(j)
        self.assertLessEqual(p.start_dt, self.scheduler.queue[-1].start_dt)

    def test_schedule_job_fails_when_project_ends(self):
        p = Project(start_dt=datetime(2021, 3, 2), expiration_dt=datetime(2021, 3, 10), id="P2")
        j = Job(duration=timedelta(hours=10), delay="1d", machine_id="M2", project=p)
        self.assertRaises(ValueError, self.scheduler.schedule_job, j)

    def test_find_available_resources_for_machine_not_found(self):
        self.assertEqual([], self.empty_scheduler.find_available_resources_for_machine("M1"))
        self.assertEqual([], self.scheduler.find_available_resources_for_machine("M0"))

    def test_find_available_resources_for_machine_found(self):
        self.assertEqual([self.r1_m1, self.r2_m1, self.r3_m1],
                         self.scheduler.find_available_resources_for_machine("M1"))
        self.assertEqual([self.r1_m2, self.r2_m2, self.r3_m2],
                         self.scheduler.find_available_resources_for_machine("M2"))

    def test_find_available_resources_for_machine_and_dt_not_found(self):
        self.assertEqual([], self.empty_scheduler.find_available_resources_for_machine_and_dt(
            "M1", datetime(2021, 4, 1, 8)))
        self.assertEqual([], self.scheduler.find_available_resources_for_machine_and_dt("M0", datetime(2021, 4, 1, 8)))
        self.assertEqual([], self.scheduler.find_available_resources_for_machine_and_dt("M1", datetime(2021, 4, 3, 8)))

    def test_find_available_resources_for_machine_and_dt_found(self):
        self.assertEqual([self.r2_m1, self.r3_m1],
                         self.scheduler.find_available_resources_for_machine_and_dt("M1", datetime(2021, 4, 1, 15)))
        self.assertEqual([self.r3_m2],
                         self.scheduler.find_available_resources_for_machine_and_dt("M2", datetime(2021, 4, 2, 3)))

    def test_find_earliest_resource_not_found(self):
        self.assertEqual(None, self.empty_scheduler.find_earliest_resource("M1", datetime(2021, 3, 28, 8)))
        self.assertEqual(None, self.scheduler.find_earliest_resource("M0", datetime(2021, 3, 28, 8)))
        self.assertEqual(None, self.scheduler.find_earliest_resource("M1", datetime(2021, 4, 20, 8)))

    def test_find_earliest_resource_found(self):
        self.assertEqual(self.r1_m1, self.scheduler.find_earliest_resource("M1", datetime(2021, 4, 1, 8)))

    def find_fastest_start_dt(self):
        self.assertEqual(self.j1.project.start_dt, self.empty_scheduler.find_fastest_start_dt(self.j1))
        self.assertEqual(self.r1_m1, self.scheduler.find_fastest_start_dt(self.j1))

    def test_find_scheduled_jobs_not_found(self):
        self.assertEqual([], self.empty_scheduler.find_scheduled_jobs(self.project.id))
        self.assertEqual([], self.scheduler.find_scheduled_jobs("P0"))

    def test_find_scheduled_jobs_found(self):
        self.assertEqual(self.queue_init, self.scheduler.find_scheduled_jobs(self.project.id))

    def test_find_last_scheduled_job_not_found(self):
        self.assertEqual(None, self.empty_scheduler.find_last_scheduled_job(self.project.id))
        self.assertEqual(None, self.scheduler.find_last_scheduled_job("P0"))

    def test_find_last_scheduled_job_found(self):
        self.assertEqual(datetime(2021, 4, 1, 14), self.scheduler.find_last_scheduled_job(self.project.id).end_dt)
