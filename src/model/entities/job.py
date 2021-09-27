from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta, datetime

from src.model.entities.project import Project
from src.utils.date_utils import next_n_days, next_weekday


@dataclass(frozen=True)
class Job:
    """
    Class that represents a job entity in a project
    """
    duration: timedelta  # Job duration
    machine_id: str  # Job's machine ID
    delay: str  # Job delay after finish
    project: Project  # Job's project ID
    previous_machines: list[str]  # List of previous machine IDs

    def is_previous_job(self, job: Job) -> bool:
        """
        Checks if the given job has to be scheduled before self
        @param job: job to check
        @return: True if has to be scheduled before, else False
        """
        return self.project.id == job.project.id and job.machine_id in self.previous_machines


@dataclass(frozen=True)
class ScheduledJob(Job):
    """
    Class that represents a scheduled job
    """
    end_dt: datetime  # Scheduled job's end date
    start_dt: datetime  # Scheduled job's start date

    def __post_init__(self):
        if self.start_dt > self.end_dt:
            raise ValueError("Scheduled job end date can not be before scheduled job start date")

    def __le__(self, other: ScheduledJob) -> bool:
        return self.end_dt <= other.end_dt

    def __lt__(self, other: ScheduledJob) -> bool:
        return self.end_dt < other.end_dt

    def parse_delay(self) -> datetime:
        """
        Adds delay to the given date
        @return: end datetime with added delay
        """
        if self.delay[-1].lower() == 'd':
            return next_n_days(self.end_dt, int(self.delay[:-1]))
        else:
            return next_weekday(self.end_dt, 0)
