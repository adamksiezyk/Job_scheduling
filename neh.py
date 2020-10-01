import lib
import functools
import numpy as np
import pandas as pd
import copy
from datetime import datetime, timedelta

FIRST_SHIFT = datetime.strptime('06:00', "%H:%M").time()
SECOND_SHIFT = datetime.strptime('14:00', "%H:%M").time()
THIRD_SHIFT = datetime.strptime('22:00', "%H:%M").time()

def neh(job_list, machine_dict, workers_dict, start_date):
    # TODO functional style

    queue = list()
    # Sort the job list from longest to shortest duration
    jobs_sorted = sorted(job_list.items(), key=lambda job: lib.sum_dict_val(job[1], timedelta(0)), reverse=True)
    queue.append(jobs_sorted[0])
    for i in range(1, len(job_list)):
        c_min = pd.Timedelta.max#timedelta.max
        for j in range(i + 1):
            queue_tmp = lib.insert(queue, j, jobs_sorted[i])
            c_matrix = calculate_makespan(queue_tmp, len(job_list), machine_dict, workers_dict, start_date)
            c_tmp = get_c_max(start_date, c_matrix)
            if c_min > c_tmp:
                queue_best = queue_tmp
                c_min = c_tmp
        queue = queue_best
    c_matrix = calculate_makespan(queue, len(job_list), machine_dict, workers_dict, start_date)
    c_max = get_c_max(start_date, c_matrix)
    return queue, c_max, c_matrix

# Calculate makespan of queue
def calculate_makespan(queue, jobs_amount, machine_dict, workers_dict, start_date, c_matrix_parallel_job=None):
    # TODO functional style
    # Queue for the next day
    queue_next_day = list()
    # Machine amount
    machine_amount = fetch_machine_amount(machine_dict, start_date)
    # Worker amount
    workers_amount = fetch_machine_amount(workers_dict, start_date)
    # Cost matrix
    instances = lib.flatten([list(range(1, amount + 1)) for amount in machine_amount.values()])
    machines = lib.flatten([[machine] * machine_amount[machine] for machine in machine_amount])
    jobs = lib.flatten([[job_id] * len(instances) for (job_id, _) in queue])
    arrays = [[start_date] * len(jobs), jobs, machines * len(queue), instances * len(queue)]
    index = pd.MultiIndex.from_arrays(arrays=arrays, names=['Date', 'Job', 'Machine', 'Instance'])
    c_matrix = pd.DataFrame(data=[[start_date] * 2] * len(jobs), index=index, columns=['Start', 'End'])
    
    # TODO: too complicated ...
    for (job_id, job) in queue:
        for machine in job:
            operation_duration = job[machine]
            if not operation_duration: continue
            if not machine_amount[machine] or not workers_amount[machine]:
                job_next_day = lib.slice_dict(job, machine)[1]
                queue_next_day.append((job_id, job_next_day))
                break
            operation_start, operation_end, fastest_machine = get_operation_details(c_matrix, job_id, job, machine, machine_amount[machine], workers_amount[machine], operation_duration, start_date)
            if operation_end > next_shift(start_date):
                job_next_day = lib.slice_dict(job, machine)[1]
                if operation_start < next_shift(start_date):
                    c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'Start'] = operation_start
                    c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'End'] = next_shift(start_date)
                    job_next_day[machine] = (operation_end - next_shift(start_date)) * workers_amount[machine]
                queue_next_day.append((job_id, job_next_day))
                break
            c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'Start'] = operation_start
            c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'End'] = operation_end                

    c_matrix['Duration'] = c_matrix['End'] - c_matrix['Start']
    c_matrix_total = c_matrix
    if queue_next_day:
        date_next_shift = next_shift(start_date)
        c_matrix_next_shift = calculate_makespan(queue_next_day, jobs_amount, machine_dict, workers_dict, date_next_shift)
        c_matrix_total = c_matrix_total.append(c_matrix_next_shift)
    return c_matrix_total

def fetch_machine_amount(machine_dict, start_date):
    machine_amount_day = machine_dict[start_date.date()]
    if start_date.time() < FIRST_SHIFT: return lib.get_column(2)(machine_amount_day)
    elif start_date.time() < SECOND_SHIFT: return lib.get_column(0)(machine_amount_day)
    elif start_date.time() < THIRD_SHIFT: return lib.get_column(1)(machine_amount_day)
    else: return lib.get_column(2)(machine_amount_day)

def get_previous_machine_duration(c_matrix, machine, first_machine, job_id, start_date):
    return start_date if machine == first_machine else max(c_matrix.loc[(start_date, job_id), 'End'])

def get_fastest_machine(c_matrix, machine_id, machine_amount, c_matrix_parallel_job=None):
    # TODO check c_matrix_parallel_job if machine is available for the given slot
    fastest_machine = [max(c_matrix.loc[(slice(None), machine_id, instance), 'End']) for instance in range(1, machine_amount + 1)]
    return min(fastest_machine), np.argmin(fastest_machine) + 1

def get_operation_details(c_matrix, job_id, job, machine, machine_amount, workers_amount, operation_duration, start_date):
    previous_machine_duration = get_previous_machine_duration(c_matrix, machine, next(iter(job)), job_id, start_date)
    fastest_machine_duration, fastest_machine = get_fastest_machine(c_matrix.loc[start_date], machine, machine_amount)
    operation_start = max(previous_machine_duration, fastest_machine_duration)
    operation_end = operation_start + operation_duration / workers_amount
    return operation_start, operation_end, fastest_machine

def get_c_max(start_date, c_matrix):
    return pd.Timedelta(0) if c_matrix.empty else max(c_matrix['End']) - start_date

def next_shift(start_date):
    if start_date.time() < FIRST_SHIFT: return datetime.combine(start_date.date(), FIRST_SHIFT)
    elif start_date.time() < SECOND_SHIFT: return datetime.combine(start_date.date(), SECOND_SHIFT)
    elif start_date.time() < THIRD_SHIFT: return datetime.combine(start_date.date(), THIRD_SHIFT)
    else : return datetime.combine(start_date.date() + timedelta(days=1), FIRST_SHIFT)
