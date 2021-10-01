from src.controller import controller


def main() -> None:
    """
    Main function, starts scheduling
    @return: None
    """
    path_jobs = '..\\resources\\Linia_VA_08_2021.xlsx'
    sheet_jobs = 'Linia VA'
    path_resources = '..\\resources\\WorkCalendar_08_2021.xlsx'
    sheet_resources = 'Sheet1'
    algorithm = 0
    if algorithm == 0:
        controller.schedule_genetic(path_jobs=path_jobs, sheet_jobs=sheet_jobs, path_resources=path_resources,
                                    sheet_resources=sheet_resources, population_size=10, generation_limit=20)
    elif algorithm == 1:
        controller.schedule_neh(path_jobs=path_jobs, sheet_jobs=sheet_jobs, path_resources=path_resources,
                                sheet_resources=sheet_resources)


if __name__ == "__main__":
    main()
