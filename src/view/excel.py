from dataclasses import asdict

import pandas as pd

from src.model.entities.scheduler import Scheduler


def scheduler_to_excel(scheduler: Scheduler, name: str) -> None:
    df = scheduler_to_df(scheduler)
    df.to_excel(name)


def scheduler_to_df(scheduler: Scheduler) -> pd.DataFrame:
    jobs_dicts = [asdict(job) for job in scheduler.queue]
    return pd.DataFrame.from_records(jobs_dicts)


def df_to_excel(df: pd.DataFrame, name: str) -> None:
    df.to_excel(name)
