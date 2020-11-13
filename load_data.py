def load_data(path_jobs, sheet_name_jobs, path_resources, sheet_name_resources, amount=None):

    # Load jobs
    jobs = load_jobs(path_jobs, sheet_name_jobs, amount)

    # Machines and workers availability, fixed for now
    resources = load_resources(path_resources, sheet_name_resources)
    return jobs, resources


def load_jobs(path, sheet_name, amount):
    import pandas as pd
    from datetime import datetime, time

    data = pd.read_excel(path, sheet_name=sheet_name)
    amount = amount if amount else len(data)
    jobs = {}
    for i in range(len(data)):
        row = data.iloc[i]
        col = data.columns
        # Take only recent jobs, not too old and don't go to far to the future
        if row[col[1]] >= pd.Timestamp(year=2020, month=11, day=12):
            # Rearrange them to fit the desired machine scheduling pattern pattern
            date = datetime.combine(row[col[1]].date(), time(hour=6))
            jobs[(row[col[0]], date)] = [[{col[3]: (row[col[3]], '0d'), col[4]: (row[col[4]], '1d')}, "&",
                                          {col[5]: (row[col[5]], '0d'), col[6]: (row[col[6]], '1d')}], ">",
                                         {col[7]: (row[col[7]], '1t'), col[8]: (row[col[8]], '2d'), col[9]: (row[col[9]], '2d'), col[10]: (row[col[10]], '0d')}]
        if len(jobs) == amount:
            break
    return jobs


def load_resources(path, sheet_name):
    import pandas as pd

    data = pd.read_excel(path, sheet_name)
    col = data.columns
    resources = {}
    for i in range(0, len(data), 3):
        resources[data.iloc[i][col[0]].date()] = {
            machine: [data.iloc[i][machine], data.iloc[i+1]
                      [machine], data.iloc[i+2][machine]]
            for machine in col[2:-1]
        }
    return resources


if __name__ == '__main__':
    load_resources('WorkCalendar.xlsx', 'Sheet1')
