from src.model.algorithms.brute_force import BruteForceScheduler
from src.model.algorithms.genetic import GeneticScheduler
from src.model.algorithms.neh import NehScheduler
from src.model.db.excel.fetch import fetch_all_resources, fetch_jobs_list
from src.model.db.excel.load_data import load_data
from src.model.entities.job import Job
from src.model.entities.resource import Resource
from src.model.entities.scheduler import Scheduler
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


def schedule_genetic(path_jobs: str, sheet_jobs: str, path_resources: str, sheet_resources: str, population_size: int,
                     generation_limit: int) -> Scheduler:
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
    jobs = get_jobs(path_jobs, sheet_jobs)
    resources = get_resources(path_resources, sheet_resources)

    algorithm = GeneticScheduler(jobs, resources)
    solution = algorithm.optimize(population_size=population_size, generation_limit=generation_limit,
                                  mutation_amount=15, mutation_probability=0.6)
    scheduler = Scheduler(resources)
    [scheduler.schedule_job(jobs[i]) for i in solution]
    print(f"Optimal solution takes: {scheduler.calculate_queue_duration()}")

    # gantt_chart(scheduler.queue)
    return scheduler


def schedule_brute_force(path_jobs: str, sheet_jobs: str, path_resources: str, sheet_resources: str) -> Scheduler:
    """
    Performs the scheduling using a brute force algorithm
    @param path_jobs: path to jobs excel
    @param sheet_jobs: spreadsheet name of the jobs
    @param path_resources: path to resources excel
    @param sheet_resources: spreadsheet name of the resources
    @return: scheduler with scheduled jobs
    """
    jobs = get_jobs(path_jobs, sheet_jobs)[:3]
    resources = get_resources(path_resources, sheet_resources)

    algorithm = BruteForceScheduler(jobs, resources)
    solution = algorithm.optimize()
    scheduler = Scheduler(resources)
    [scheduler.schedule_job(job) for job in solution]
    print(f"Optimal solution takes: {scheduler.calculate_queue_duration()}")

    gantt_chart(scheduler.queue)
    return scheduler


def schedule_neh(path_jobs: str, sheet_jobs: str, path_resources: str, sheet_resources: str) -> Scheduler:
    """
    Performs the scheduling using a NEH algorithm
    @param path_jobs: path to jobs excel
    @param sheet_jobs: spreadsheet name of the jobs
    @param path_resources: path to resources excel
    @param sheet_resources: spreadsheet name of the resources
    @return: scheduler with scheduled jobs
    """
    jobs = get_jobs(path_jobs, sheet_jobs)
    resources = get_resources(path_resources, sheet_resources)

    algorithm = NehScheduler(jobs, resources)
    solution = algorithm.optimize()
    scheduler = Scheduler(resources)
    [scheduler.schedule_job(job) for job in solution]
    print(f"Optimal solution takes: {scheduler.calculate_queue_duration()}")

    gantt_chart(scheduler.queue)
    return scheduler
