from src.model.algorithms.brute_force import BruteForceScheduler
from src.model.algorithms.genetic import GeneticScheduler
from src.model.algorithms.neh import NehScheduler
from src.model.db.excel.fetch import fetch_all_resources, fetch_jobs_list
from src.model.db.excel.load_data import load_data
from src.model.entities.job import Job
from src.model.entities.resource import Resource
from src.model.entities.scheduler import Scheduler
from src.view.excel import scheduler_to_excel
from src.view.gantt_chart import gantt_chart


def get_jobs(path, sheet) -> list[Job]:
    """
    Loads jobs and represents them
    @param path: path to xlsx file
    @param sheet: spreadsheet name
    @return:
    """
    data = load_data(path, sheet)
    jobs = fetch_jobs_list(data)
    return jobs


def get_resources(path, sheet) -> dict[str, list[Resource]]:
    """
    Loads resources and represents them
    @param path: path to xlsx file
    @param sheet: spreadsheet name
    @return: None
    """
    resource_data = load_data(path, sheet)
    resources = fetch_all_resources(resource_data)
    return resources


def schedule(algorithm_class, algorithm_kwargs: dict, path_jobs: str, sheet_jobs: str,
             path_resources: str, sheet_resources: str) -> Scheduler:
    """
    Performs the scheduling using the given algorithm
    @param algorithm_class: a scheduling algorithm class
    @param algorithm_kwargs: keyword arguments for the algorithm optimize method
    @param path_jobs: path to the excel file with jobs
    @param sheet_jobs: name of the sheet with jobs
    @param path_resources: path to the excel file with resources
    @param sheet_resources: name of the sheet with resources
    @return: scheduler with scheduled jobs
    """
    jobs = get_jobs(path_jobs, sheet_jobs)
    resources = get_resources(path_resources, sheet_resources)

    algorithm = algorithm_class(jobs, resources)
    solution = algorithm.optimize(**algorithm_kwargs)
    scheduler = Scheduler(resources)
    for i in solution:
        scheduler.schedule_job(jobs[i])
    print(f"Optimal solution takes: {scheduler.calculate_queue_duration()}")

    gantt_chart(scheduler.queue)
    scheduler_to_excel(scheduler, "..\\resources\\output.xlsx")
    return scheduler


def schedule_genetic(path_jobs: str, sheet_jobs: str, path_resources: str, sheet_resources: str, population_size: int,
                     generation_limit: int) -> None:
    """
    Performs the scheduling using a genetic algorithm
    @param path_jobs: path to the excel file with jobs
    @param sheet_jobs: name of the sheet with jobs
    @param path_resources: path to the excel file with resources
    @param sheet_resources: name of the sheet with resources
    @param population_size: size of the population
    @param generation_limit: limit of generations
    @return: scheduler with scheduled jobs
    """
    kwargs = {
        'population_size': population_size,
        'generation_limit': generation_limit,
        'mutation_amount': 15,
        'mutation_probability': 0.6
    }
    schedule(algorithm_class=GeneticScheduler, algorithm_kwargs=kwargs, path_jobs=path_jobs, sheet_jobs=sheet_jobs,
             path_resources=path_resources, sheet_resources=sheet_resources)


def schedule_brute_force(path_jobs: str, sheet_jobs: str, path_resources: str, sheet_resources: str) -> None:
    """
    Performs the scheduling using a brute force algorithm
    @param path_jobs: path to jobs excel
    @param sheet_jobs: spreadsheet name of the jobs
    @param path_resources: path to resources excel
    @param sheet_resources: spreadsheet name of the resources
    @return: scheduler with scheduled jobs
    """
    schedule(algorithm_class=BruteForceScheduler, algorithm_kwargs={}, path_jobs=path_jobs, sheet_jobs=sheet_jobs,
             path_resources=path_resources, sheet_resources=sheet_resources)


def schedule_neh(path_jobs: str, sheet_jobs: str, path_resources: str, sheet_resources: str) -> None:
    """
    Performs the scheduling using a NEH algorithm
    @param path_jobs: path to jobs excel
    @param sheet_jobs: spreadsheet name of the jobs
    @param path_resources: path to resources excel
    @param sheet_resources: spreadsheet name of the resources
    @return: scheduler with scheduled jobs
    """
    schedule(algorithm_class=NehScheduler, algorithm_kwargs={}, path_jobs=path_jobs, sheet_jobs=sheet_jobs,
             path_resources=path_resources, sheet_resources=sheet_resources)
