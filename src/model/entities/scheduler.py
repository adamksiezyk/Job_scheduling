from dataclasses import dataclass, replace
from datetime import datetime
from typing import Optional

from src.model.entities.job import ScheduledJob, Job
from src.model.entities.resource import Resource


@dataclass
class Scheduler:
    queue: list[ScheduledJob]  # Queue of scheduled jobs
    resources: list[Resource]  # List of available resources

    def schedule_job(self, job: Job) -> None:
        # TODO too big
        previous_job = self.find_last_scheduled_job(job.project.project_id)
        previous_job_end = datetime.min if previous_job is None else previous_job.parse_delay()
        fastest_resource = self.find_earliest_resource(job.machine_id, previous_job_end)
        if fastest_resource is None:
            raise ValueError("No resources available")

        ###########################
        _job_end_dt = fastest_resource.start_dt + job.duration
        job_end_dt = _job_end_dt if _job_end_dt <= fastest_resource.end_dt else fastest_resource.end_dt
        new_resource = replace(fastest_resource, start_dt=job_end_dt) if job_end_dt < fastest_resource.end_dt else None
        scheduled_job = ScheduledJob(job_end_dt, fastest_resource.start_dt, job.delay, job.project, job.machine_id)
        ###########################

        job_end_dt = fastest_resource.start_dt + job.duration
        if job_end_dt > job.project.expiration_dt:
            ValueError("Job duration exceeded project's expiration date")

        if job_end_dt <= fastest_resource.end_dt:
            # This resource is enough
            new_resource = replace(fastest_resource, start_dt=job_end_dt)
            self.resources.remove(fastest_resource)
            self.resources.append(new_resource)
            scheduled_job = ScheduledJob(job.duration, job.machine_id, job.delay, job.project, job_end_dt,
                                         fastest_resource.start_dt)
            self.queue.append(scheduled_job)
        else:
            # TODO resource is not enough
            pass

    def find_available_resources(self, machine_id: str) -> list[Resource]:
        """
        Returns a list of available resources for the given machine
        @param machine_id: machine ID
        @return: list of available resources
        """
        return [r for r in self.resources if r.machine_id == machine_id]

    def find_earliest_resource(self, machine_id: str, start_dt: datetime) -> Optional[Resource]:
        """
        Returns the earliest available resource for the given machine and start datetime
        @param machine_id: machine ID
        @param start_dt: start datetime
        @return: earliest available resource
        """
        available_resources = self.find_available_resources(machine_id)
        if available_resources is None:
            return None
        return min(resource for resource in available_resources if resource.end_dt < start_dt)

    def find_scheduled_jobs(self, project_id: str) -> list[ScheduledJob]:
        """
        Returns the scheduled jobs for the given project
        @param project_id: project ID
        @return: list of scheduled jobs
        """
        return [j for j in self.queue if j.project.project_id == project_id]

    def find_last_scheduled_job(self, project_id: str) -> Optional[ScheduledJob]:
        """
        Returns the last scheduled job for the give project
        @param project_id: project ID
        @return: last scheduled job
        """
        previous_jobs = self.find_scheduled_jobs(project_id)
        return max(previous_jobs) if previous_jobs else None
