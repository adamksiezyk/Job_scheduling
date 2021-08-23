from src.controller.controller import get_jobs, get_resources


def main() -> None:
    """
    Main function, starts scheduling
    @return: None
    """
    data_path = '..\\Linia_VA.xlsx'
    sheet_name = 'Linia VA'
    get_jobs(data_path, sheet_name)

    resources_path = '..\\WorkCalendar.xlsx'
    resources_sheet = 'Sheet1'
    get_resources(resources_path, resources_sheet)


if __name__ == "__main__":
    main()
