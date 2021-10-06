from dataclasses import replace
from datetime import datetime, timedelta
from unittest import TestCase

from src.model.entities.job import ScheduledJob, Job
from src.model.entities.project import Project
from src.model.entities.resource import Resource, Resources
from src.model.entities.scheduler import Scheduler


class TestScheduler(TestCase):
    def setUp(self) -> None:
        self.empty_scheduler = Scheduler(Resources())

        self.r1_m1 = Resource(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 14),
                              worker_amount=2)
        self.r2_m1 = Resource(start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 22),
                              worker_amount=2)
        self.r3_m1 = Resource(start_dt=datetime(2021, 4, 1, 22), end_dt=datetime(2021, 4, 2, 6),
                              worker_amount=2)
        self.r1_m2 = Resource(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 14),
                              worker_amount=2)
        self.r2_m2 = Resource(start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 22),
                              worker_amount=2)
        self.r3_m2 = Resource(start_dt=datetime(2021, 4, 1, 22), end_dt=datetime(2021, 4, 2, 6),
                              worker_amount=2)
        self.resources_init = Resources()
        self.resources_init.append("M1", self.r1_m1)
        self.resources_init.append("M1", self.r2_m1)
        self.resources_init.append("M1", self.r3_m1)
        self.resources_init.append("M2", self.r1_m2)
        self.resources_init.append("M2", self.r2_m2)
        self.resources_init.append("M2", self.r3_m2)

        self.project = Project(start_dt=datetime(2021, 3, 28, 6), expiration_dt=datetime(2021, 4, 10), id="P1")
        self.j1 = ScheduledJob(start_dt=datetime(2021, 3, 28, 6), end_dt=datetime(2021, 3, 28, 14),
                               duration=timedelta(hours=8), delay='0d', machine_id="M1", project=self.project,
                               previous_machines=[])
        self.j2 = ScheduledJob(start_dt=datetime(2021, 3, 28, 6), end_dt=datetime(2021, 3, 28, 14),
                               duration=timedelta(hours=8), delay='1d', machine_id="M2", project=self.project,
                               previous_machines=[])
        self.j3 = ScheduledJob(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 14),
                               duration=timedelta(hours=8), delay='0d', machine_id="M1", project=self.project,
                               previous_machines=["M1"])
        self.j4 = ScheduledJob(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 10),
                               duration=timedelta(hours=4), delay='0d', machine_id="M2", project=self.project,
                               previous_machines=["M2"])
        self.queue_init = [self.j1, self.j2, self.j3, self.j4]

        self.scheduler = Scheduler(resources=self.resources_init)
        self.scheduler.queue = [*self.queue_init]

    def test_calculate_queue_duration(self):
        self.assertEqual(datetime(2021, 4, 1, 14).timestamp(), self.scheduler.calculate_queue_duration())

    def test_schedule_job_no_resources(self):
        j1 = Job(duration=timedelta(hours=6), delay="1d", machine_id="M0", project=self.project, previous_machines=[])
        j2 = Job(duration=timedelta(hours=6), delay="0d", machine_id="M1", project=self.project, previous_machines=[])
        self.assertRaises(ValueError, self.empty_scheduler.schedule_job, j2)
        self.assertRaises(ValueError, self.scheduler.schedule_job, j1)

    def test_schedule_job_one_resource_is_enough(self):
        j = Job(duration=timedelta(hours=6), delay="0d", machine_id="M1", project=self.project,
                previous_machines=["M1"])
        s = ScheduledJob(duration=j.duration, delay=j.delay, machine_id=j.machine_id, project=j.project,
                         start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 17), previous_machines=["M1"])
        r = replace(self.r2_m1, start_dt=s.end_dt)
        used_resources = {
            'M1': {self.resources_init.get_resources("M1").index(self.r2_m1): r},
            'M2': {}
        }
        self.queue_init.append(s)

        self.scheduler.schedule_job(j)
        self.assertEqual(used_resources, self.scheduler.used_resources)
        self.assertEqual(self.queue_init, self.scheduler.queue)

    def test_schedule_job_one_resource_is_not_enough(self):
        j = Job(duration=timedelta(hours=20), delay="1d", machine_id="M2", project=self.project,
                previous_machines=["M1", "M2"])
        s1 = ScheduledJob(duration=timedelta(hours=16), delay="0d", machine_id=j.machine_id, project=j.project,
                          start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 22),
                          previous_machines=j.previous_machines)
        s2 = ScheduledJob(duration=timedelta(hours=4), delay=j.delay, machine_id=j.machine_id, project=j.project,
                          start_dt=datetime(2021, 4, 1, 22), end_dt=datetime(2021, 4, 2, 0),
                          previous_machines=j.previous_machines)
        r = replace(self.r3_m2, start_dt=s2.end_dt)
        used_resources = {
            'M1': {},
            'M2': {
                self.resources_init.get_resources("M2").index(self.r2_m2): None,
                self.resources_init.get_resources("M2").index(self.r3_m2): r,
            }
        }
        self.queue_init.append(s1)
        self.queue_init.append(s2)

        self.scheduler.schedule_job(j)
        self.assertEqual(used_resources, self.scheduler.used_resources)
        self.assertEqual(self.queue_init, self.scheduler.queue)

    def test_schedule_job_before_project_start(self):
        p = Project(start_dt=datetime(2021, 4, 2), expiration_dt=datetime(2021, 4, 10), id="P1")
        j = Job(duration=timedelta(hours=10), delay="1d", machine_id="M2", project=p, previous_machines=[])
        self.scheduler.schedule_job(j)
        self.assertLessEqual(p.start_dt, self.scheduler.queue[-1].start_dt)

    def test_schedule_job_fails_when_project_ends(self):
        p = Project(start_dt=datetime(2021, 3, 2), expiration_dt=datetime(2021, 3, 10), id="P2")
        j = Job(duration=timedelta(hours=10), delay="1d", machine_id="M2", project=p, previous_machines=[])
        self.assertRaises(ValueError, self.scheduler.schedule_job, j)

    def test_create_scheduled_entities_for_single_resource(self):
        job = Job(duration=timedelta(hours=6), delay="0d", machine_id="M1", project=self.project,
                  previous_machines=["M1"])
        job_start_dt = datetime(2021, 4, 1, 6)
        scheduled_job = ScheduledJob(duration=job.duration, delay=job.delay, machine_id=job.machine_id,
                                     project=job.project, start_dt=job_start_dt,
                                     end_dt=datetime(2021, 4, 1, 9), previous_machines=job.previous_machines)
        resource = replace(self.r1_m1, start_dt=scheduled_job.end_dt)
        next_job = None
        expected_output = (scheduled_job, resource, next_job)
        self.assertEqual(expected_output, self.scheduler.create_scheduled_entities(job=job, resource=self.r1_m1,
                                                                                   job_start_dt=job_start_dt))

    def test_create_scheduled_entities_for_multiple_resources(self):
        job = Job(duration=timedelta(hours=20), delay="0d", machine_id="M1", project=self.project,
                  previous_machines=[])
        job_start_dt = datetime(2021, 4, 1, 6)
        scheduled_job = ScheduledJob(duration=timedelta(hours=16), delay="0d", machine_id=job.machine_id,
                                     project=job.project, start_dt=job_start_dt,
                                     end_dt=datetime(2021, 4, 1, 14), previous_machines=job.previous_machines)
        resource = None
        next_job = replace(job, duration=timedelta(hours=4))
        expected_output = (scheduled_job, resource, next_job)
        self.assertEqual(expected_output, self.scheduler.create_scheduled_entities(job=job, resource=self.r1_m1,
                                                                                   job_start_dt=job_start_dt))

    def test_find_earliest_resource_not_found(self):
        self.assertRaises(ValueError, self.empty_scheduler.find_earliest_resource, "M1", datetime(2021, 3, 28, 8))
        self.assertRaises(ValueError, self.scheduler.find_earliest_resource, "M0", datetime(2021, 3, 28, 8))
        self.assertRaises(ValueError, self.scheduler.find_earliest_resource, "M1", datetime(2021, 4, 20, 8))

    def test_find_earliest_resource_found(self):
        self.assertEqual((0, self.r1_m1), self.scheduler.find_earliest_resource("M1", datetime(2021, 4, 1, 8)))

    def test_find_previous_job_end_dt(self):
        self.assertEqual(self.j1.project.start_dt, self.empty_scheduler.find_previous_job_end_dt(self.j1))
        self.assertEqual(datetime(2021, 3, 28, 6), self.scheduler.find_previous_job_end_dt(self.j1))

    def test_find_jobs_to_wait_for_not_found(self):
        self.assertEqual([], self.empty_scheduler.find_jobs_to_wait_for(self.j2))
        p0 = Project(start_dt=datetime.now(), expiration_dt=datetime.now(), id="P0")
        j0 = Job(project=p0, machine_id="M0", previous_machines=[], delay="1d", duration=timedelta(hours=2))
        self.assertEqual([], self.scheduler.find_jobs_to_wait_for(j0))

    def test_find_jobs_to_wait_for_found(self):
        self.assertEqual([self.j1, self.j3], self.scheduler.find_jobs_to_wait_for(self.j3))
        self.assertEqual([self.j2, self.j4], self.scheduler.find_jobs_to_wait_for(self.j4))

    def test_find_previous_job_not_found(self):
        self.assertEqual(None, self.empty_scheduler.find_previous_job(self.j1))
        self.assertEqual(None, self.scheduler.find_previous_job(self.j1))

    def test_find_previous_job_found(self):
        self.assertEqual(datetime(2021, 4, 1, 14), self.scheduler.find_previous_job(self.j3).end_dt)
