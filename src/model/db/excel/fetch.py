import functools
from datetime import datetime, time, timedelta
from functools import reduce

import pandas as pd

from src.model.entities.job import Job
from src.model.entities.project import Project
from src.model.entities.resource import Resource
from src.model.entities.scheduler import Scheduler


def fetch_project(project: pd.Series) -> Project:
    """
    Fetches project from series
    @param project: pandas Series
    @return: project
    """
    return Project(datetime.combine(project.iloc[1].date(), time(hour=6)), project.iloc[2].to_pydatetime(),
                   project.iloc[0])


def fetch_jobs_list(data_frame: pd.DataFrame) -> list[Job]:
    """
    Fetches jobs from dataframe
    @param data_frame: pandas dataframe
    @return: list of jobs
    """
    return reduce(lambda ans, row: ans + fetch_jobs_in_project(row[1]), data_frame.iterrows(), [])


def fetch_jobs_dict(data_frame: pd.DataFrame) -> dict[Project, list[Job]]:
    """
    Fetches a dict of project, job list pairs from the dataframe
    @param data_frame: pandas dataframe
    @return: dict of project, job list pairs
    """
    return {fetch_project(row): order_jobs(fetch_jobs_in_project(row)) for _, row in data_frame.iterrows()}


def fetch_jobs_dict_from_list(jobs_list: list[Job]) -> dict[Project, list[Job]]:
    """
    Fetches jobs dict from jobs list
    @param jobs_list: list of jobs
    @return: a dict of jobs
    """
    jobs_dict = functools.reduce(lambda ans, job: append_to_dict_value_or_create_value(ans, job.project, job),
                                 jobs_list, {})
    return {key: order_jobs(value) for key, value in jobs_dict.items()}


def fetch_jobs_in_project(series: pd.Series) -> list[Job]:
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
    previous_machines = {
        '1.VA.NAB': [],
        '2.VA.RS': ['1.VA.NAB'],
        '3.VA.BKOM': [],
        '4.VA.MKOM': ['3.VA.BKOM'],
        '5.VA.KOMWKŁ': ['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM'],
        '6.VA.MKONC': ['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ'],
        '7.VA.OWIE': ['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC'],
        '8.VA.MBAT': ['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC', '7.VA.OWIE']
    }
    return [Job(duration, str(machine), delay, project, previous_machines[machine])
            for machine, duration, delay in zip(machines, durations, delays)]


def append_to_dict_value_or_create_value(_dict: dict[Project, list[Job]], key: Project, value: Job
                                         ) -> dict[Project, list[Job]]:
    return {**_dict, key: _dict[key] + [value]} if key in _dict else {**_dict, key: [value]}


def order_jobs(_jobs: list[Job]) -> list[Job]:
    jobs = [*_jobs]
    ordered_jobs = []
    while jobs:
        jobs_next = []
        for job in jobs:
            if Scheduler.check_if_previous_machines_are_scheduled(job, ordered_jobs):
                ordered_jobs.append(job)
            else:
                jobs_next.append(job)
        jobs = jobs_next
    return ordered_jobs


def fetch_all_resources(data_frame: pd.DataFrame) -> dict[str, list[Resource]]:
    """
    Fetches resources from dataframe
    @param data_frame: pandas dataframe
    @return: list of resources
    """
    resources = {name: [] for name in data_frame.columns[2:-1]}
    return functools.reduce(
        lambda ans, elem: {key: value + fetch_resources(elem[1])[key] for key, value in ans.items()},
        data_frame.iterrows(),
        resources)


def fetch_resources(series: pd.Series) -> dict[str, list[Resource]]:
    """
    Fetches resources from series
    @param series: pandas series
    @return: list of resources
    """
    start_dt = series.iloc[0].to_pydatetime()
    end_dt = series.iloc[1].to_pydatetime()
    resources = series.iloc[2:-1]
    return {name: [Resource(start_dt=start_dt, end_dt=end_dt, worker_amount=int(amount.split('x')[1]))
                   for _ in range(int(amount.split('x')[0]))] for name, amount in resources.iteritems()}
