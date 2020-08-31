import neh
import json

def parse_machines(machine_string):
    return json.loads(machine_string)

def parse_jobs(job_string):
    job_list = job_string.split('&')
    return list(map(json.loads, job_list))

# Production start & end
date_start = '2020-08-24'
date_end = '2020-08-30'

# Each index is a machine, the value represents the amount
# machine_amount = [3, 2, 3]   # M1: 3, M2: 2, M3: 3
machine_amount = ('{"2020-08-24": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                    '"2020-08-25": [["2x2", "1x2", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                    '"2020-08-26": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                    '"2020-08-27": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                    '"2020-08-28": [["2x2", "0x0", "x"], ["4x1", "2x1", "0x0"], ["2x2", "0x0", "x"]], ' +
                    '"2020-08-29": [["1x0,5", "0x0", "x"], ["1x1,5", "0x0", "0x0"], ["0x0", "0x0", "x"]]}')
# List of jobs, each dict is a job, key = machine, value = duration
jobs = ('{"M1": 3, "M2": 2, "M3": 4}' + '&' +
        '{"M1": 2, "M2": 5, "M3": 1}' + '&' +
        '{"M3": 3, "M1": 0, "M2": 2}' + '&' +
        '{"M3": 2, "M2": 0, "M1": 3}')

# jobs = parse_jobs(jobs)
# queue, c_matrix = neh.neh(jobs, machine_amount)
# print(queue)
# print(c_matrix)
machines = parse_machines(machine_amount)
print(machines)