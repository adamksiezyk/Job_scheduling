import neh
import plot
import json
import pandas as pd
from datetime import datetime, timedelta

def parse_machines(machine_string):
    machine_dict = json.loads(machine_string)
    machine_amount = {datetime.strptime(date, "%Y-%m-%d").date():
                        [[0 if shift == 'x' else int(shift.split('x')[0]) for shift in machine] for machine in machine_dict[date]]
                        for date in machine_dict}
    workers_amount = {datetime.strptime(date, "%Y-%m-%d").date():
                        [[0 if shift == 'x' else float(shift.split('x')[1]) for shift in machine] for machine in machine_dict[date]]
                        for date in machine_dict}
    return machine_amount, workers_amount

def parse_jobs(job_string):
    # job_list = job_string.split('&')
    job_dict = json.loads(job_string)
    job_dict_fetched = {job: {machine: timedelta(hours=job_dict[job][machine]) for machine in job_dict[job]} for job in job_dict}
    return job_dict_fetched

# Production start & end
date_start = '2020-08-24'
date_end = '2020-08-30'

machines = ('{  "2020-08-31": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                '"2020-09-01": [["2x2", "1x2", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                '"2020-09-02": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                '"2020-09-03": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                '"2020-09-04": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                '"2020-09-05": [["1x0.5", "0x0", "x"], ["1x1.5", "0x0", "0x0"], ["0x0", "0x0", "x"]], ' +
                '"2020-09-06": [["5x3", "5x3", "5x3"], ["5x3", "5x3", "5x3"], ["5x3", "5x3", "5x3"]], ' +
                '"2020-09-07": [["3x2", "3x2", "3x2"], ["3x2", "3x2", "3x2"], ["3x2", "3x2", "3x2"]]}')

# List of jobs, each dict is a job, key = machine, value = duration
jobs = ('{"Zaginanie": {"M1": 18, "M2": 12, "M3": 24}, ' +
        '"Spawanie": {"M1": 12, "M2": 18, "M3": 12} ,' +
        '"Cynkowanie": {"M1": 24, "M2": 18, "M3": 18}}')

machine_dict, workers_dict = parse_machines(machines)
jobs = parse_jobs(jobs)
queue, (c_matrix, duration) = neh.neh(jobs, machine_dict, workers_dict, datetime.strptime('2020-08-31 13:09', "%Y-%m-%d %H:%M"))
# c_matrix, queue = neh.calculate_makespan(list(jobs.items()), 3, machine_dict, datetime.strptime('2020-08-31 13:09', "%Y-%m-%d %H:%M"))
print('\n', duration, '\n')
print(queue, '\n')
with pd.option_context('display.max_rows', None):  # more options can be specified also
    print(c_matrix.loc[c_matrix['Duration'] != timedelta(0)])

plot.plot(c_matrix)