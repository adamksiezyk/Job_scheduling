from src.model.db.excel.fetch import fetch_all_resources, fetch_all_jobs
from src.model.db.excel.load_data import load_data
from src.model.entities.job import Job
from src.model.entities.resource import Resource


def get_jobs(path, sheet) -> list[Job]:
    """
    Loads jobs and represents them
    @param path: path to xlsx file
    @param sheet: spreadsheet name
    @return:
    """
    data = load_data(path, sheet)
    jobs = fetch_all_jobs(data)
    return jobs


def get_resources(path, sheet) -> list[Resource]:
    """
    Loads resources and represents them
    @param path: path to xlsx file
    @param sheet: spreadsheet name
    @return: None
    """
    resource_data = load_data(path, sheet)
    resources = fetch_all_resources(resource_data.iloc[:, :-1])
    return resources
