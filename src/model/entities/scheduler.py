from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Optional

from src.model.entities.job import ScheduledJob, Job
from src.model.entities.resource import Resource


class Scheduler:
    def __init__(self, resources: dict[str, list[Resource]]):
        # List of all resources
        self.RESOURCES = resources
        # Dict to indicate used resources
        self.used_resources: dict[str, dict[int, Optional[Resource]]] = {key: {} for key in resources.keys()}
        # Queue of scheduled jobs
        self.queue: list[ScheduledJob] = []

    def calculate_queue_duration(self) -> float:
        """
        Returns the duration of the queue
        @return: queue duration
        """
        return max(self.queue).end_dt.timestamp()

    def schedule_job(self, job: Job) -> None:
        # TODO too big
        fastest_start_dt = self.find_fastest_start_dt(job)
        fastest_resource_idx, fastest_resource = self.find_earliest_resource(job.machine_id, fastest_start_dt)
        if fastest_resource is None:
            raise ValueError("No resources available")

        job_start_dt = max(fastest_start_dt, fastest_resource.start_dt)
        job_end_dt = job_start_dt + (job.duration / fastest_resource.worker_amount)
        if job_end_dt > job.project.expiration_dt:
            raise ValueError("Job duration exceeded project's expiration date")

        if job_end_dt <= fastest_resource.end_dt:
            # This resource is enough
            new_resource = replace(fastest_resource, start_dt=job_end_dt)
            self.used_resources[job.machine_id][fastest_resource_idx] = new_resource
            scheduled_job = ScheduledJob(job.duration, job.machine_id, job.delay, job.project, job_end_dt, job_start_dt)
            self.queue.append(scheduled_job)
        else:
            # Next resource is needed
            self.used_resources[job.machine_id][fastest_resource_idx] = None
            new_job_end_dt = fastest_resource.end_dt
            new_duration = (new_job_end_dt - job_start_dt) * fastest_resource.worker_amount
            scheduled_job = ScheduledJob(new_duration, job.machine_id, '0d', job.project, new_job_end_dt, job_start_dt)
            self.queue.append(scheduled_job)
            new_job = Job(job.duration - new_duration, job.machine_id, job.delay, job.project)
            self.schedule_job(new_job)

    def get_available_resource(self, machine_id: str, index: int, start_dt: datetime) -> Optional[Resource]:
        try:
            r = self.used_resources[machine_id][index]
        except KeyError:
            r = self.RESOURCES[machine_id][index]
        return None if r is None or r.end_dt <= start_dt else r

    def find_earliest_resource(self, machine_id: str, start_dt: datetime) -> tuple[Optional[int], Optional[Resource]]:
        """
        Returns the earliest available resource for the given machine and start datetime
        @param machine_id: machine ID
        @param start_dt: start datetime
        @return: earliest available resource and it's index
        """
        # Use a generator to stop looping after first available resource is found
        try:
            return next(((i, self.get_available_resource(machine_id, i, start_dt))
                         for i, r in enumerate(self.RESOURCES[machine_id])
                         if self.get_available_resource(machine_id, i, start_dt) is not None))
        except (KeyError, StopIteration):
            return None, None

    def find_fastest_start_dt(self, job: Job) -> datetime:
        """
        Returns the fastest start datetime of the given job
        @param job: job to schedule
        @return: fastest start datetime
        """
        previous_job = self.find_last_scheduled_job(job.project.id)
        previous_job_end = datetime.min if previous_job is None else previous_job.parse_delay()
        return max(previous_job_end, job.project.start_dt)

    def find_last_scheduled_job(self, project_id: str) -> Optional[ScheduledJob]:
        """
        Returns the last scheduled job for the give project
        @param project_id: project ID
        @return: last scheduled job
        """
        previous_jobs = self.find_scheduled_jobs(project_id)
        return max(previous_jobs) if previous_jobs else None

    def find_scheduled_jobs(self, project_id: str) -> list[ScheduledJob]:
        """
        Returns the scheduled jobs for the given project
        @param project_id: project ID
        @return: list of scheduled jobs
        """
        return [j for j in self.queue if j.project.id == project_id]
