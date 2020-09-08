import lib
import functools
import numpy as np
import copy
from datetime import datetime, timedelta

def neh(job_list, machine_dict, start_date):
    # TODO functional style

    queue = list()
    # Sort the job list from longest to shortest duration
    jobs_sorted = sorted(job_list.items(), key=lambda job: lib.sum_dict_val(job[1], timedelta(0)), reverse=True)
    queue.append(jobs_sorted[0])
    for i in range(1, len(job_list)):
        c_min = timedelta.max
        for j in range(i + 1):
            queue_tmp = lib.insert(queue, j, jobs_sorted[i])
            _, c_tmp = calculate_makespan(queue_tmp, len(job_list), machine_dict, start_date)
            if c_min > c_tmp:
                queue_best = queue_tmp
                c_min = c_tmp
        queue = queue_best
    return queue, calculate_makespan(queue, len(job_list), machine_dict, start_date)

# Calculate makespan of queue
def calculate_makespan(_queue, jobs_amount, machine_dict, start_date, previous_max=timedelta(hours=0)):
    # TODO functional style
    queue = copy.deepcopy(_queue)
    # Queue for the next day
    queue_next_day = list()
    # Machine amount
    machine_amount = machine_dict[start_date.strftime("%Y-%m-%d")]
    # Cost matrix
    c_matrix = [[[start_date] * amount for amount in machine_amount] for job in range(jobs_amount)]
    
    # TODO: too complicated ...
    for (job_id, job) in queue:
        for machine in job.keys():
            machine_id = int(machine[1:]) - 1
            operation_duration = job[machine]
            if operation_duration == timedelta(hours=0): continue
            if machine == next(iter(job)): previous_machine_duration = start_date
            else: previous_machine_duration = lib.max_2_dim(c_matrix[job_id])
            fastest_machine_duration, fastest_machine = find_fastest_machine(c_matrix, machine_id)
            operation_end = max(previous_machine_duration, fastest_machine_duration) + operation_duration
            if operation_end < next_day_midnight(start_date):
                c_matrix[job_id][machine_id][fastest_machine] = operation_end
            elif fastest_machine_duration >= next_day_midnight(start_date):
                job_next_day = lib.slice_dict(job, machine)[1]
                job_next_day[machine] = operation_end - next_day_midnight(start_date)
                queue_next_day.append((job_id, job_next_day))
                break
            else:
                c_matrix[job_id][machine_id][fastest_machine] = next_day_midnight(start_date)
                job_next_day = lib.slice_dict(job, machine)[1]
                job_next_day[machine] = operation_end - next_day_midnight(start_date)
                queue_next_day.append((job_id, job_next_day))
                break

    c_max = lib.max_3_dim(c_matrix) - start_date + previous_max
    c_matrix_total = [c_matrix]

    if queue_next_day:
        date_next_day = next_day_midnight(start_date)
        c_matrix_next_day, c_max_next_day = calculate_makespan(queue_next_day, jobs_amount, machine_dict, date_next_day, c_max)
        c_matrix_total += c_matrix_next_day
        c_max_total = c_max_next_day
    else: c_max_total = c_max
    return c_matrix_total, c_max_total

def find_fastest_machine(c_matrix, machine_id):
    machine_amount = len(c_matrix[0][machine_id])
    fastest_machine = functools.reduce( lambda a, m:
                                            lib.append(a, max(lib.pipeline(c_matrix, [lib.get_column(machine_id), lib.get_column(m)]))),
                                        range(machine_amount),
                                        [])
    return min(fastest_machine), np.argmin(fastest_machine)

def next_day_midnight(start_day):
    next_day = start_day + timedelta(days=1)
    return datetime.combine(next_day.date(), datetime.min.time())

# c = [[[18, 0], [24, 0], [0]], [[0, 12], [0, 24], [0]], [[0, 24], [0, 0], [0]]]
# fastest_machine = find_fastest_machine(c, 0)
# print(fastest_machine)