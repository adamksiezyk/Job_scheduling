from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Optional

from src.model.entities.job import ScheduledJob, Job
from src.model.entities.resource import Resource, Resources, UsedResources


class Scheduler:
    def __init__(self, resources: Resources):
        self.resources = UsedResources(resources)
        self.queue: list[ScheduledJob] = []

    def calculate_queue_duration(self) -> float:
        """
        Returns the duration of the queue
        @return: queue duration
        """
        return max(self.queue).end_dt.timestamp()

    def schedule_job(self, job: Job) -> None:
        previous_job_end_dt = self.find_previous_job_end_dt(job)
        earliest_resource_idx, earliest_resource = self.resources.find_earliest_resource(job.machine_id,
                                                                                         previous_job_end_dt)
        job_start_dt = max(previous_job_end_dt, earliest_resource.start_dt)
        scheduled_job, new_resource, new_job = self.create_scheduled_entities(job, earliest_resource, job_start_dt)
        if scheduled_job.end_dt > job.project.expiration_dt:
            raise ValueError("Job duration exceeded project's expiration date")
        self.queue.append(scheduled_job)
        self.resources.use_resource(job.machine_id, earliest_resource_idx, new_resource)
        if new_job is not None:
            self.schedule_job(new_job)

    @staticmethod
    def create_scheduled_entities(job: Job, resource: Resource, job_start_dt: datetime
                                  ) -> tuple[ScheduledJob, Optional[Resource], Optional[Job]]:
        """
        Returns the entities that are mandatory for scheduling a job
        @param job: job to schedule
        @param resource: resource to be used
        @param job_start_dt: job's start datetime
        @return: scheduled job, new, used resource, next job to be scheduled if more resources are needed
        """
        whole_job_end_dt = job_start_dt + (job.duration / resource.worker_amount)
        next_resource_needed = whole_job_end_dt > resource.end_dt
        job_end_dt = resource.end_dt if next_resource_needed else whole_job_end_dt
        job_duration = (job_end_dt - job_start_dt) * resource.worker_amount if next_resource_needed else job.duration
        job_delay = "0d" if next_resource_needed else job.delay

        scheduled_job = ScheduledJob(duration=job_duration, machine_id=job.machine_id,
                                     previous_machines=job.previous_machines, delay=job_delay, project=job.project,
                                     end_dt=job_end_dt, start_dt=job_start_dt)
        new_resource = None if job_end_dt == resource.end_dt else replace(resource, start_dt=job_end_dt)
        new_job = replace(job, duration=job.duration - job_duration) if next_resource_needed else None
        return scheduled_job, new_resource, new_job

    def find_previous_job_end_dt(self, job: Job) -> datetime:
        """
        Returns the fastest start datetime of the given job
        @param job: job to schedule
        @return: fastest start datetime
        """
        previous_job = self.find_previous_job(job)
        return job.project.start_dt if previous_job is None else previous_job.parse_delay()

    def find_previous_job(self, job: Job) -> Optional[ScheduledJob]:
        """
        Returns the last scheduled job for the give project
        @param job: current job
        @return: last scheduled job
        """
        previous_jobs = self.find_jobs_to_wait_for(job)
        if not self.check_if_previous_machines_are_scheduled(job, previous_jobs):
            raise ValueError("Wrong scheduling order")
        return max(previous_jobs) if previous_jobs else None

    def find_jobs_to_wait_for(self, job: Job) -> list[ScheduledJob]:
        """
        Returns the scheduled jobs for the given project
        @param job: current Job
        @return: list of scheduled jobs
        """
        return [j for j in self.queue if job.is_previous_job(j)]

    @staticmethod
    def check_if_previous_machines_are_scheduled(job: Job, scheduled_jobs: list[Job]) -> bool:
        """
        Checks if all previous machines for the given job are scheduled
        @param job: job to check
        @param scheduled_jobs: list of scheduled jobs
        @return: True if previous machines are scheduled else False
        """
        return set(job.previous_machines) <= set(j.machine_id for j in scheduled_jobs)
