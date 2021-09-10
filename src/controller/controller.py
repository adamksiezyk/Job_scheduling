from src.model.algorithms.genetic import SchedulingGeneticAlgorithm
from src.model.db.excel.fetch import fetch_all_resources, fetch_all_jobs
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
    resources = fetch_all_resources(resource_data)
    return resources


def schedule(path_jobs: str, sheet_jobs: str, path_resources: str, sheet_resources: str, population_size: int,
             generation_limit: int) -> Scheduler:
    """
    Performs the scheduling
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

    algorithm = SchedulingGeneticAlgorithm(jobs, resources)
    solution = algorithm.optimize(population_size=population_size, generation_limit=generation_limit,
                                  mutation_amount=15, mutation_probability=0.6)
    scheduler = Scheduler(resources)
    [scheduler.schedule_job(jobs[i]) for i in solution]
    print(f"Optimal solution: {solution}, takes: {scheduler.calculate_queue_duration()}")

    gantt_chart(scheduler.queue)
    return scheduler
