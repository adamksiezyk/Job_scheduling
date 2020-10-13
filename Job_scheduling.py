import lib
import load_data
import neh
import plot
import json
import ast
import functools
import pandas as pd
from copy import deepcopy
from datetime import datetime, timedelta

def parse_machines(machine_string):
    # machine_dict = json.loads(machine_string)
    machine_dict = ast.literal_eval(machine_string)
    machine_amount = {datetime.strptime(date, "%Y-%m-%d").date():
                        {machine: [0 if shift == 'x' else int(shift.split('x')[0]) for shift in machine_dict[date][machine]]
                        for machine in machine_dict[date]}
                        for date in machine_dict}
    workers_amount = {datetime.strptime(date, "%Y-%m-%d").date():
                        {machine: [0 if shift == 'x' else float(shift.split('x')[1]) for shift in machine_dict[date][machine]]
                        for machine in machine_dict[date]}
                        for date in machine_dict}
    return machine_amount, workers_amount

def parse_jobs(jobs):
    if isinstance(jobs, list):
        return functools.reduce(
            lambda ans, job: lib.append(ans, parse_jobs(job)),
            jobs,
            [])
    elif jobs == '>' or jobs == '&': return jobs
    elif isinstance(jobs, dict):
        return {(job_id, datetime.strptime(job_start, "%Y-%m-%d %H:%M:%S")): parse_machines_in_job(machines)
                    for (job_id, job_start), machines in jobs.items()}
    else: raise RuntimeError('Wrong jobs format')

def parse_machines_in_job(machines):
    if isinstance(machines, list):
        return functools.reduce(
            lambda ans, machine: lib.append(ans, parse_machines_in_job(machine)),
            machines,
            [])
    elif machines == '&' or machines == '>': return machines
    elif isinstance(machines, dict):
        return {machine: timedelta(hours=time)
            for machine, time in machines.items()}
    else: raise RuntimeError('Wrong machines format')

def schedule(job, machine_dict, workers_dict, start_date, c_matrix_old=pd.DataFrame()):
    if isinstance(job, dict):
        return neh.neh(job, machine_dict, workers_dict, start_date, c_matrix_old)
    elif isinstance(job, list):
        if job[1] == '&':
            # TODO we assume the two jobs don't use the same machines!
            result_0 = schedule(job[0], machine_dict, workers_dict, start_date)
            result_2 = schedule(job[2], machine_dict, workers_dict, start_date, result_0[2])
            c_matrix = result_2[2]
            return lib.append(result_0[0], result_2[0]), max(result_0[1], result_2[1]), c_matrix
        elif job[1] == '>':
            result_0 = schedule(job[0], machine_dict, workers_dict, start_date)
            result_2 = schedule(job[2], machine_dict, workers_dict, start_date + result_0[1], result_0[2])
            c_matrix = result_2[2]
            return lib.append(result_0[0], result_2[0]), result_0[1] + result_2[1], c_matrix
    else:
        raise RuntimeError('Wrong jobs format.')

# def schedule(job_list, machine_dict, workers_dict, start_date):
    # date = deepcopy(start_date)
    # queue = []
    # c_matrix = pd.DataFrame()
    # for jobs in job_list:
    #     result = neh.neh(jobs, machine_dict, workers_dict, date)
    #     queue.append(result[0])
    #     date += result[1]
    #     c_matrix = c_matrix.append(result[2])
    # return queue, date - start_date, c_matrix

# Production start & end
date_start = '2020-08-24'
date_end = '2020-08-30'
# Dict of machines and workers key = date, value = list of machines, each machine consists of 3 shifts in format 2x1, 2-machines x 1-workers
# machines = ('{  "2020-08-31": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
#                 '"2020-09-01": [["2x2", "1x2", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
#                 '"2020-09-02": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
#                 '"2020-09-03": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
#                 '"2020-09-04": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
#                 '"2020-09-05": [["1x0.5", "0x0", "x"], ["1x1.5", "0x0", "0x0"], ["0x0", "0x0", "x"]], ' +
#                 '"2020-09-06": [["5x3", "5x3", "5x3"], ["5x3", "5x3", "5x3"], ["5x3", "5x3", "5x3"]], ' +
#                 '"2020-09-07": [["3x2", "3x2", "3x2"], ["3x2", "3x2", "3x2"], ["3x2", "3x2", "3x2"]]}')

# List of jobs, each dict is a job, key = machine, value = duration
# jobs = ('[[{"Zaginanie": {"M1": 18, "M2": 12, "M3": 24}, ' +
#         '"Spawanie": {"M1": 12, "M2": 18, "M3": 12}, ' +
#         '"Cynkowanie": {"M1": 24, "M2": 18, "M3": 18}}, ">", ' +
#         '{"Lakierowanie": {"M1": 18, "M2": 12, "M3": 24}}], ">", ' +
#         '{"Pakowanie": {"M1": 12, "M2": 18, "M3": 12}, ' +
#         '"Wysyłka": {"M1": 24, "M2": 18, "M3": 18}}]')

# start_date = datetime.strptime('2020-08-31 13:09', "%Y-%m-%d %H:%M")

jobs, machines, start_date = load_data.load_data('Linia_VA.xlsx', 'Linia VA')

machine_dict, workers_dict = parse_machines(machines)
jobs = parse_jobs(ast.literal_eval(jobs))
import time
t1 = time.time()
queue, duration, c_matrix = schedule(jobs, machine_dict, workers_dict, start_date)
t2 = time.time()
print('\n', queue, '\n')
print(duration, '\n')
with pd.option_context('display.max_rows', None):  # more options can be specified also
    print(c_matrix.loc[c_matrix.Duration.ne(timedelta(0))])
plot.plot(c_matrix)
print(t2-t1)