from src.model.db.excel.fetch import fetch_all_resources, fetch_all_jobs
from src.model.db.excel.load_data import load_data


def get_jobs(path, sheet) -> None:
    """
    Loads jobs and represents them
    @param path: path to xlsx file
    @param sheet: spreadsheet name
    @return: None
    """
    data = load_data(path, sheet)
    jobs = fetch_all_jobs(data)
    print(jobs)


def get_resources(path, sheet) -> None:
    """
    Loads resources and represents them
    @param path: path to xlsx file
    @param sheet: spreadsheet name
    @return: None
    """
    resource_data = load_data(path, sheet)
    resources = fetch_all_resources(resource_data.iloc[:, :-1])
    print(resources)
