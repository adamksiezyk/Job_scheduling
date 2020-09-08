import neh
import json
from datetime import datetime, timedelta

def parse_machines(machine_string):
    return json.loads(machine_string)

def parse_jobs(job_string):
    job_list = job_string.split('&')
    job_dict = {job: json.loads(job_list[job]) for job in range(len(job_list))}
    job_dict_fetched = {job: {machine: timedelta(hours=job_dict[job][machine]) for machine in job_dict[job]} for job in job_dict}
    return job_dict_fetched

# Production start & end
date_start = '2020-08-24'
date_end = '2020-08-30'

# machine_amount = ('{"2020-08-31": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                    # '"2020-09-01": [["2x2", "1x2", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                    # '"2020-09-02": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                    # '"2020-09-03": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                    # '"2020-09-04": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                    # '"2020-09-05": [["1x0,5", "0x0", "x"], ["1x1,5", "0x0", "0x0"], ["0x0", "0x0", "x"]]}')
machines = ('{"2020-08-31": [2, 2, 1], ' +
            '"2020-09-01": [1, 1, 2], ' +
            '"2020-09-02": [2, 1, 1], ' +
            '"2020-09-03": [2, 2, 2], ' +
            '"2020-09-04": [1, 1, 1], ' +
            '"2020-09-05": [5, 5, 5], ' +
            '"2020-09-06": [5, 5, 5], ' +
            '"2020-09-07": [3, 3, 3]}')
# List of jobs, each dict is a job, key = machine, value = duration
jobs = ('{"M1": 18, "M2": 12, "M3": 24}' + '&' +
        '{"M1": 12, "M2": 18, "M3": 12}' + '&' +
        '{"M1": 24, "M2": 18, "M3": 18}')

machine_dict = parse_machines(machines)
jobs = parse_jobs(jobs)
queue, c_matrix = neh.neh(jobs, machine_dict, datetime.strptime('2020-08-31 13:09', "%Y-%m-%d %H:%M"))
# c_matrix, queue = neh.calculate_makespan(list(jobs.items()), 3, machine_dict, datetime.strptime('2020-08-31 13:09', "%Y-%m-%d %H:%M"))
print('\n', c_matrix[1], '\n')
print(queue, '\n')
for matrix in c_matrix[0]:
    for job in matrix:
        print(job)
    print()