from datetime import datetime, time, timedelta
from functools import reduce

import pandas as pd

from src.model.entities.job import Job
from src.model.entities.project import Project
from src.model.entities.resource import Resource


def fetch_project(project: pd.Series) -> Project:
    """
    Fetches project from series
    @param project: pandas Series
    @return: project
    """
    return Project(datetime.combine(project.iloc[1].date(), time(hour=6)), project.iloc[2].to_pydatetime(),
                   project.iloc[0])


def fetch_all_jobs(data_frame: pd.DataFrame) -> list[Job]:
    """
    Fetches jobs from dataframe
    @param data_frame: pandas dataframe
    @return: list of jobs
    """
    return reduce(lambda ans, row: ans + fetch_jobs(row[1]), data_frame.iterrows(), [])


def fetch_jobs(series: pd.Series) -> list[Job]:
    """
    Fetches jobs from series
    @param series: pandas series
    @return: list of jobs
    """
    project = fetch_project(series)
    jobs = series.iloc[3:-1]
    machines = [str(name) for i, (name, _) in enumerate(jobs.iteritems()) if i % 2 == 1]
    durations = [timedelta(hours=duration) for i, (_, duration) in enumerate(jobs.iteritems()) if i % 2 == 1]
    delays = [str(delay) for i, (_, delay) in enumerate(jobs.iteritems()) if i % 2 == 0]
    return [Job(duration, str(machine), delay, project)
            for machine, duration, delay in zip(machines, durations, delays)]


def fetch_all_resources(data_frame: pd.DataFrame) -> list[Resource]:
    """
    Fetches resources from dataframe
    @param data_frame: pandas dataframe
    @return: list of resources
    """
    return reduce(lambda ans, row: ans + fetch_resources(row[1]), data_frame.iterrows(), [])


def fetch_resources(series: pd.Series) -> list[Resource]:
    """
    Fetches resources from series
    @param series: pandas series
    @return: list of resources
    """
    start_dt = series.iloc[0].to_pydatetime()
    end_dt = series.iloc[1].to_pydatetime()
    resources = series.iloc[2:-1]
    return [Resource(start_dt, end_dt, name, int(amount.split('x')[1]))
            for name, amount in resources.iteritems() for _ in range(int(amount.split('x')[0]))]
