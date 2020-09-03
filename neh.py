import functools
import numpy as np
from datetime import datetime, timedelta

def neh(job_list, machine_dict, start_date):
    # TODO functional style

    queue = list()
    # Sort the job list from longest to shortest duration
    jobs_sorted = sorted(job_list.items(), key=lambda job: calculate_job_makespan(job[1]), reverse=True)
    queue.append(jobs_sorted[0])
    for i in range(1, len(job_list)):
        c_min = float('inf')
        for j in range(i + 1):
            queue_tmp = insert(queue, j, jobs_sorted[i])
            c_matrix, c_tmp = calculate_makespan(queue_tmp, len(job_list), machine_dict, start_date)
            if c_min > c_tmp:
                queue_best = queue_tmp
                c_min = c_tmp
        queue = queue_best
    return queue, calculate_makespan(queue, len(job_list), machine_dict, start_date)

# Duration of job over all machines
def calculate_job_makespan(job):
    return functools.reduce(lambda sum, key: sum + job[key], job, 0)

# Insert into immutable list
def insert(list, index, value):
    _list = list[:]
    _list.insert(index, value)
    return _list

# Calculate makespan of queue
def calculate_makespan(_queue, jobs_amount, machine_dict, start_date, previous_max=0):
    # TODO functional style
    queue = _queue.copy()
    # Queue for the next day
    queue_next_day = list()
    # Machine amount
    machine_amount = machine_dict[start_date.strftime("%Y-%m-%d")]
    # Cost matrix
    c_matrix = [[[0] * amount for amount in machine_amount] for job in range(jobs_amount)]
    
    # TODO: too complicated ...
    for (job_id, job) in queue:
        job_next_day = job.copy()
        for machine in job.keys():
            machine_id = int(machine[1:]) - 1
            operation_duration = job[machine]
            if operation_duration == 0: continue
            if machine == next(iter(job)): previous_machine_duration = 0
            else: previous_machine_duration = max([max(_machine) for _machine in c_matrix[job_id]])
            fastest_machine_duration = min([max([_job[machine_id][m] for _job in c_matrix]) for m in range(machine_amount[machine_id])])
            fastest_machine = np.argmin([max([_job[machine_id][m] for _job in c_matrix]) for m in range(machine_amount[machine_id])])
            if max(previous_machine_duration, fastest_machine_duration) + operation_duration <= 24:
                c_matrix[job_id][machine_id][fastest_machine] = max(previous_machine_duration, fastest_machine_duration) + operation_duration
                del job_next_day[machine] # Or use slice_dictionary() in else?
            else:
                c_matrix[job_id][machine_id][fastest_machine] = 24
                job_next_day[machine] = max(previous_machine_duration, fastest_machine_duration) + operation_duration - 24
                queue_next_day.append((job_id, job_next_day))
                break
    c_max = max([max([max(machines) for machines in job]) for job in c_matrix]) + previous_max
    c_matrix_total = [c_matrix]
    if queue_next_day:
        date_next_day = start_date + timedelta(days=1)
        c_matrix_next_day, c_max_next_day = calculate_makespan(queue_next_day, jobs_amount, machine_dict, date_next_day, c_max)
        c_matrix_total += c_matrix_next_day
        c_max_total = c_max_next_day
    else: c_max_total = c_max
    return c_matrix_total, c_max_total

def slice_dict(_dictionary, key_slice):
    dictionary = _dictionary.copy()
    for key in _dictionary:
        if key == key_slice: return dictionary
        del dictionary[key]
    return dictionary