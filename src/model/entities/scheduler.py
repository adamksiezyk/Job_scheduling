from dataclasses import dataclass, replace, field
from datetime import datetime
from typing import Optional

from src.model.entities.job import ScheduledJob, Job
from src.model.entities.resource import Resource


@dataclass
class Scheduler:
    queue: list[ScheduledJob] = field(default_factory=list)  # Queue of scheduled jobs
    resources: list[Resource] = field(default_factory=list)  # List of available resources

    def schedule_job(self, job: Job) -> None:
        # TODO too big
        fastest_start_dt = self.find_fastest_start_dt(job)
        fastest_resource = self.find_earliest_resource(job.machine_id, fastest_start_dt)
        if fastest_resource is None:
            raise ValueError("No resources available")

        job_start_dt = max(fastest_start_dt, fastest_resource.start_dt)
        job_end_dt = job_start_dt + (job.duration / fastest_resource.worker_amount)
        if job_end_dt > job.project.expiration_dt:
            ValueError("Job duration exceeded project's expiration date")

        if job_end_dt <= fastest_resource.end_dt:
            # This resource is enough
            new_resource = replace(fastest_resource, start_dt=job_end_dt)
            self.resources.remove(fastest_resource)
            self.resources.append(new_resource)
            scheduled_job = ScheduledJob(job.duration, job.machine_id, job.delay, job.project, job_end_dt, job_start_dt)
            self.queue.append(scheduled_job)
        else:
            # Next resource is needed
            self.resources.remove(fastest_resource)
            new_job_end_dt = fastest_resource.end_dt
            new_duration = new_job_end_dt - job_start_dt
            scheduled_job = ScheduledJob(new_duration, job.machine_id, '0d', job.project, new_job_end_dt, job_start_dt)
            self.queue.append(scheduled_job)
            new_job = Job(job.duration - new_duration, job.machine_id, job.delay, job.project)
            self.schedule_job(new_job)

    def find_available_resources_for_machine(self, machine_id: str) -> list[Resource]:
        """
        Returns a list of available resources for the given machine
        @param machine_id: machine ID
        @return: list of available resources
        """
        return [r for r in self.resources if r.machine_id == machine_id]

    def find_available_resources_for_machine_and_dt(self, machine_id: str, start_dt: datetime) -> list[Resource]:
        """
        Returns a list of available resources for the given machine and datetime
        @param machine_id: machine ID
        @param start_dt: start datetime
        @return: list of available resources
        """
        return [r for r in self.find_available_resources_for_machine(machine_id) if start_dt < r.end_dt]

    def find_earliest_resource(self, machine_id: str, start_dt: datetime) -> Optional[Resource]:
        """
        Returns the earliest available resource for the given machine and start datetime
        @param machine_id: machine ID
        @param start_dt: start datetime
        @return: earliest available resource
        """
        available_resources = self.find_available_resources_for_machine_and_dt(machine_id, start_dt)
        if not available_resources:
            return None
        return min(available_resources)

    def find_fastest_start_dt(self, job) -> datetime:
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
        return max(previous_jobs, key=lambda j: j.end_dt) if previous_jobs else None

    def find_scheduled_jobs(self, project_id: str) -> list[ScheduledJob]:
        """
        Returns the scheduled jobs for the given project
        @param project_id: project ID
        @return: list of scheduled jobs
        """
        return [j for j in self.queue if j.project.id == project_id]
