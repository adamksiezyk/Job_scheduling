def load_data(path, sheet_name):
    import pandas as pd
    from datetime import datetime, timedelta

    data = pd.read_excel(path, sheet_name=sheet_name)

    jobs = {}
    for i in range(10):
        dat = data.iloc[i]
        col = data.columns
        jobs[dat[col[0]]] = [[{col[3]: dat[col[3]], col[4]: dat[col[4]]}, '&',
                {col[5]: dat[col[5]], col[6]: dat[col[6]]}], '>',
            {col[7]: dat[col[7]], col[8]: dat[col[8]], col[9]: dat[col[9]], col[10]: dat[col[10]]}]
    # TODO start_date for every job
    start_date = data.iloc[0]['Start produkcji WT']
    machines = str({datetime.strftime(start_date + timedelta(days=delta), "%Y-%m-%d"): {machine: ["3x1", "3x1", "3x1"] for machine in col[3:]} for delta in range(100)})
    
    return str(jobs), machines, start_date