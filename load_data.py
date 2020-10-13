def load_data(path, sheet_name):
    import functools
    import pandas as pd
    from datetime import datetime, timedelta

    data = pd.read_excel(path, sheet_name=sheet_name)

    jobs = []
    for i in range(10):
        dat = data.iloc[i]
        col = data.columns
        if pd.Timestamp(year=2020, month=12, day=1) > dat[col[1]] > pd.Timestamp(year=2020, month=9, day=1):
            jobs.append({(dat[col[0]], str(dat[col[1]])): [[{col[3]: dat[col[3]], col[4]: dat[col[4]]}, '&',
                    {col[5]: dat[col[5]], col[6]: dat[col[6]]}], '>',
                {col[7]: dat[col[7]], col[8]: dat[col[8]], col[9]: dat[col[9]], col[10]: dat[col[10]]}]})
    start_date = min(data.iloc[0:6]['Start produkcji WT'])
    machines = str({datetime.strftime(start_date + timedelta(days=delta), "%Y-%m-%d"): {machine: ["3x1", "3x1", "3x1"] for machine in col[3:]} for delta in range(100)})
    # jobs = [[[[[jobs[0], '&', jobs[1]], '&', jobs[2]], '&', jobs[3]], '&', jobs[4]], '&', jobs[5]]
    jobs = functools.reduce(lambda ans, job: [ans, '&', job], jobs[2:], [jobs[0], '&', jobs[1]])
    return str(jobs), machines, start_date