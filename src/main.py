from src.controller import controller


def main() -> None:
    """
    Main function, starts scheduling
    @return: None
    """
    path_jobs = '..\\Linia_VA.xlsx'
    sheet_jobs = 'Linia VA'
    path_resources = '..\\WorkCalendar.xlsx'
    sheet_resources = 'Sheet1'
    # controller.schedule_genetic(path_jobs=path_jobs, sheet_jobs=sheet_jobs, path_resources=path_resources,
    #                             sheet_resources=sheet_resources, population_size=10, generation_limit=20)
    controller.schedule_neh(path_jobs=path_jobs, sheet_jobs=sheet_jobs, path_resources=path_resources,
                            sheet_resources=sheet_resources)


if __name__ == "__main__":
    main()
