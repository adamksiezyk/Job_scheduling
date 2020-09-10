import lib
import functools
import numpy as np
import pandas as pd
import copy
from datetime import datetime, timedelta

FIRST_SHIFT = datetime.strptime('06:00', "%H:%M").time()
SECOND_SHIFT = datetime.strptime('14:00', "%H:%M").time()
THIRD_SHIFT = datetime.strptime('22:00', "%H:%M").time()

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
    machine_amount = fetch_machine_amount(machine_dict, start_date)
    # Cost matrix
    instances = lib.flatten([list(range(1, amount + 1)) for amount in machine_amount])
    machines = lib.flatten([['M' + str(machine_id + 1)] * machine_amount[machine_id] for machine_id in range(len(machine_amount))])
    jobs = lib.flatten([[job_id] * len(instances) for (job_id, _) in queue])
    arrays = [[start_date] * len(jobs), jobs, machines * len(queue), instances * len(queue)]
    index = pd.MultiIndex.from_arrays(arrays=arrays, names=['Date', 'Job', 'Machine', 'Instance'])
    c_matrix = pd.DataFrame(data=[[start_date] * 2] * len(jobs), index=index, columns=['Start', 'End'])
    # Max cost
    c_max = timedelta.min
    
    # TODO: too complicated ...
    for (job_id, job) in queue:
        for machine in job.keys():
            machine_id = int(machine[1:]) - 1
            operation_duration = job[machine]
            if not operation_duration: continue
            if not machine_amount[machine_id]:
                job_next_day = lib.slice_dict(job, machine)[1]
                queue_next_day.append((job_id, job_next_day))
                if c_max < next_shift(start_date) - start_date + previous_max: c_max = next_shift(start_date) - start_date + previous_max
                break
            if machine == next(iter(job)): previous_machine_duration = start_date
            else: previous_machine_duration = max(c_matrix.loc[(start_date, job_id), 'End'])
            fastest_machine_duration, fastest_machine = find_fastest_machine(c_matrix.loc[start_date], machine, machine_amount[machine_id])
            operation_start = max(previous_machine_duration, fastest_machine_duration)
            operation_end = operation_start + operation_duration
            if operation_end <= next_shift(start_date):
                c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'Start'] = operation_start
                c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'End'] = operation_end
                if c_max < operation_end - start_date + previous_max: c_max = operation_end - start_date + previous_max
            elif fastest_machine_duration >= next_shift(start_date):
                job_next_day = lib.slice_dict(job, machine)[1]
                queue_next_day.append((job_id, job_next_day))
                if c_max < next_shift(start_date) - start_date + previous_max: c_max = next_shift(start_date) - start_date + previous_max
                break
            else:
                c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'Start'] = operation_start
                c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'End'] = next_shift(start_date)
                job_next_day = lib.slice_dict(job, machine)[1]
                job_next_day[machine] = operation_end - next_shift(start_date)
                queue_next_day.append((job_id, job_next_day))
                if c_max < next_shift(start_date) - start_date + previous_max: c_max = next_shift(start_date) - start_date + previous_max
                break

    c_matrix_total = c_matrix
    if queue_next_day:
        date_next_shift = next_shift(start_date)
        c_matrix_next_shift, c_max_next_shift = calculate_makespan(queue_next_day, jobs_amount, machine_dict, date_next_shift, c_max)
        c_matrix_total = c_matrix_total.append(c_matrix_next_shift)
        c_max_total = c_max_next_shift
    else: c_max_total = c_max
    c_matrix_total['Duration'] = c_matrix_total['End'] - c_matrix_total['Start']
    return c_matrix_total, c_max_total

def fetch_machine_amount(machine_dict, start_date):
    machine_amount_day = machine_dict[start_date.strftime("%Y-%m-%d")]
    if start_date.time() < FIRST_SHIFT: return lib.get_column(2)(machine_amount_day)
    elif start_date.time() < SECOND_SHIFT: return lib.get_column(0)(machine_amount_day)
    elif start_date.time() < THIRD_SHIFT: return lib.get_column(1)(machine_amount_day)
    else : return lib.get_column(2)(machine_amount_day)

def find_fastest_machine(c_matrix, machine_id, machine_amount):
    fastest_machine = [max(c_matrix.loc[(slice(None), machine_id, instance), 'End']) for instance in range(1, machine_amount + 1)]
    return min(fastest_machine), np.argmin(fastest_machine) + 1

def next_shift(start_date):
    if start_date.time() < FIRST_SHIFT: return datetime.combine(start_date.date(), FIRST_SHIFT)
    elif start_date.time() < SECOND_SHIFT: return datetime.combine(start_date.date(), SECOND_SHIFT)
    elif start_date.time() < THIRD_SHIFT: return datetime.combine(start_date.date(), THIRD_SHIFT)
    else : return datetime.combine(start_date.date() + timedelta(days=1), FIRST_SHIFT)
