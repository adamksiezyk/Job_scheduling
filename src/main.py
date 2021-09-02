from src.controller.controller import get_jobs, get_resources
from src.model.algorithms.genetic import SchedulingGeneticAlgorithm
from src.model.entities.scheduler import Scheduler


def main() -> None:
    """
    Main function, starts scheduling
    @return: None
    """
    data_path = '..\\Linia_VA.xlsx'
    sheet_name = 'Linia VA'
    jobs = get_jobs(data_path, sheet_name)[:20]

    resources_path = '..\\WorkCalendar.xlsx'
    resources_sheet = 'Sheet1'
    resources = get_resources(resources_path, resources_sheet)

    algorithm = SchedulingGeneticAlgorithm(jobs, resources)
    solution = algorithm.optimize(100, 20)
    scheduler = Scheduler(resources)
    [scheduler.schedule_job(jobs[i]) for i in solution]
    print(f"Optimal solution: {solution}, takes: {scheduler.calculate_queue_duration()}")


if __name__ == "__main__":
    main()
