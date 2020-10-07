def load_data(path, sheet_name):
    import pandas as pd
    from datetime import datetime, timedelta

    data = pd.read_excel(path, sheet_name=sheet_name)
    dat = data.iloc[0]
    col = data.columns
    jobs = str([[{dat[col[0]]: {col[3]: dat[col[3]], col[4]: dat[col[4]]}}, '&', {dat[col[0]]: {col[5]: dat[col[5]], col[6]: dat[col[6]]}}], '>', {dat[col[0]]: {col[7]: dat[col[7]], col[8]: dat[col[8]], col[9]: dat[col[9]], col[10]: dat[col[10]]}}])
    jobs = str([{dat[col[0]]:
        [[{col[3]: dat[col[3]], col[4]: dat[col[4]]}, '&',
            {col[5]: dat[col[5]], col[6]: dat[col[6]]}], '>',
        {col[7]: dat[col[7]], col[8]: dat[col[8]], col[9]: dat[col[9]], col[10]: dat[col[10]]}]}])
    # start_date = datetime.strptime('2020-09-21 13:09', "%Y-%m-%d %H:%M")
    start_date = dat['Start produkcji WT']
    machines = str({datetime.strftime(start_date + timedelta(days=delta), "%Y-%m-%d"): {machine: ["3x2", "3x2", "3x2"] for machine in col[3:]} for delta in range(100)})
    
    return jobs, machines, start_date

# data, machines = load_data('Linia_VA.xlsx', 'Linia VA')