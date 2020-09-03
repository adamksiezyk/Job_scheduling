import neh
import json
from datetime import datetime

def parse_machines(machine_string):
    return json.loads(machine_string)

def parse_jobs(job_string):
    job_list = job_string.split('&')
    return {job: json.loads(job_list[job]) for job in range(len(job_list))}

# Production start & end
date_start = '2020-08-24'
date_end = '2020-08-30'

# Each index is a machine, the value represents the amount
# machine_amount = [3, 2, 3]   # M1: 3, M2: 2, M3: 3
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
            '"2020-09-05": [5, 5, 5]}')
# List of jobs, each dict is a job, key = machine, value = duration
jobs = ('{"M1": 18, "M2": 12, "M3": 24}' + '&' +
        '{"M1": 12, "M2": 18, "M3": 12}' + '&' +
        '{"M1": 24, "M2": 18, "M3": 18}')

machine_dict = parse_machines(machines)
jobs = parse_jobs(jobs)
# queue, c_matrix = neh.neh(jobs, machine_dict, datetime.strptime('2020-08-31', "%Y-%m-%d"))
c_matrix, queue = neh.calculate_makespan(list(jobs.items()), 3, machine_dict, datetime.strptime('2020-08-31', "%Y-%m-%d"))
print(queue)
for matrix in c_matrix:
    print(matrix)