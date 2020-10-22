def load_data(path, sheet_name):
    import functools
    import pandas as pd
    from datetime import datetime, timedelta

    data = pd.read_excel(path, sheet_name=sheet_name)

    # Load first 10 jobs
    jobs = []
    for i in range(len(data)):
        dat = data.iloc[i]
        col = data.columns
        # Take only recent jobs, not too old and don't go to far to the future
        if dat[col[1]] > pd.Timestamp(year=2020, month=1, day=1):
            # Rearrange them to fit the desired machine scheduling pattern pattern
            date = dat[col[1]].to_pydatetime()
            jobs.append({(dat[col[0]], date): [[{col[3]: dat[col[3]], col[4]: dat[col[4]]}, "&",
                                                {col[5]: dat[col[5]], col[6]: dat[col[6]]}], ">",
                                               {col[7]: dat[col[7]], col[8]: dat[col[8]], col[9]: dat[col[9]], col[10]: dat[col[10]]}]})
    # Scheduling start
    start_date = min(data.loc[data['Start produkcji WT'] > pd.Timestamp(
        year=2020, month=1, day=1)]['Start produkcji WT'])
    # Machines and workers availability, fixed for now
    machines = {start_date + timedelta(days=delta): {
        machine: ["3x1", "3x1", "3x1"] for machine in col[3:]} for delta in range(365)}
    # Every project can be run parallel
    jobs = sorted(jobs, key=lambda job: list(job.keys())[0][1])
    jobs = functools.reduce(lambda ans, job: [ans, "&", job], jobs[2:], [
        jobs[0], "&", jobs[1]])
    return jobs, machines, start_date
