import lib
import functools
import numpy as np
import pandas as pd
import copy
from datetime import datetime, timedelta

FIRST_SHIFT = datetime.strptime('06:00', "%H:%M").time()
SECOND_SHIFT = datetime.strptime('14:00', "%H:%M").time()
THIRD_SHIFT = datetime.strptime('22:00', "%H:%M").time()

def neh(job_list, machine_dict, workers_dict, start_date, c_matrix_old=pd.DataFrame()):
    # TODO functional style

    queue = list()
    # Sort the job list from longest to shortest duration
    jobs_sorted = sorted(job_list.items(), key=lambda job: lib.sum_dict_val(job[1]), reverse=True)
    queue.append(jobs_sorted[0])
    for i in range(1, len(job_list)):
        c_min = pd.Timedelta.max
        for j in range(i + 1):
            queue_tmp = lib.insert(queue, j, jobs_sorted[i])
            c_matrix = calculate_makespan(queue_tmp, machine_dict, workers_dict, start_date, c_matrix_old)
            c_tmp = get_c_max(start_date, c_matrix)
            if c_min > c_tmp:
                queue_best = queue_tmp
                c_min = c_tmp
        queue = queue_best
    c_matrix = calculate_makespan(queue, machine_dict, workers_dict, start_date, c_matrix_old)
    c_max = get_c_max(start_date, c_matrix)
    return queue, c_max, c_matrix

# Calculate makespan of queue
def calculate_makespan(queue, machine_dict, workers_dict, start_date=datetime.min, c_matrix_old=pd.DataFrame()):
    # TODO functional style
    job_start_date = min([job_start for (job_id, job_start), job in queue])
    start_date = job_start_date if job_start_date > start_date else start_date
    # Queue for the next day
    queue_next_day = list()
    # Machine amount
    machine_amount = fetch_machine_amount(machine_dict, start_date)
    # Worker amount
    workers_amount = fetch_machine_amount(workers_dict, start_date)
    # Cost matrix
    instances = lib.flatten([list(range(1, amount + 1)) for amount in machine_amount.values()])
    machines = lib.flatten([[machine] * machine_amount[machine] for machine in machine_amount])
    jobs = lib.flatten([[job_id] * len(instances) for (job_id, _), __ in queue])
    arrays = [[start_date] * len(jobs), jobs, machines * len(queue), instances * len(queue)]
    index = pd.MultiIndex.from_arrays(arrays=arrays, names=['Date', 'Job', 'Machine', 'Instance'])
    c_matrix_init = pd.DataFrame(data=[[start_date] * 2] * len(jobs), index=index, columns=['Start', 'End'])
    c_matrix_init['Duration'] = c_matrix_init['End'] - c_matrix_init['Start']
    c_matrix = c_matrix_old.append(c_matrix_init)
    
    # TODO: too complicated ...
    for (job_id, job_start_date), job in queue:
        start_date = job_start_date if job_start_date > start_date else start_date
        if isinstance(job, dict):
            for machine in job:
                operation_duration = job[machine]
                if not operation_duration: continue
                if not machine_amount[machine] or not workers_amount[machine]:
                    job_next_day = lib.slice_dict(job, machine)[1]
                    queue_next_day.append((job_id, job_next_day))
                    break
                operation_start, operation_end, fastest_machine = get_operation_details(
                    c_matrix, job_id, list(job.keys()), machine, machine_amount[machine],
                    workers_amount[machine], operation_duration, start_date)
                if operation_end > next_shift(start_date):
                    job_next_day = lib.slice_dict(job, machine)[1]
                    if operation_start < next_shift(start_date):
                        c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'Start'] = operation_start
                        c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'End'] = next_shift(start_date)
                        c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'Duration'] = next_shift(start_date) - operation_start
                        job_next_day[machine] = (operation_end - next_shift(start_date)) * workers_amount[machine]
                    queue_next_day.append(((job_id, job_start_date), job_next_day))
                    break
                c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'Start'] = operation_start
                c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'End'] = operation_end
                c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'Duration'] = operation_end - operation_start

        elif isinstance(job, list):
            if job[1] == '&':
                # TODO we assume the two jobs don't use the same machines!
                result_0 = calculate_makespan(
                    [((job_id, job_start_date), job[0])], machine_dict, workers_dict, start_date,
                    c_matrix.loc[c_matrix.Duration.ne(timedelta(0))])
                result_2 = calculate_makespan(
                    [((job_id, job_start_date), job[2])], machine_dict, workers_dict, start_date, result_0)
                c_matrix = result_2
            elif job[1] == '>':
                result_0 = calculate_makespan(
                    [((job_id, job_start_date), job[0])], machine_dict, workers_dict, start_date,
                    c_matrix.loc[c_matrix.Duration.ne(timedelta(0))])
                end_date = start_date if result_0.empty or job_id not in result_0.index.get_level_values('Job') else max(result_0.loc[(slice(None), job_id), 'End'])
                result_2 = calculate_makespan(
                    [((job_id, job_start_date), job[2])], machine_dict, workers_dict, end_date, result_0)
                c_matrix = result_2
        else:
            raise RuntimeError('Wrong jobs format.')

    if queue_next_day:
        date_next_shift = next_shift(start_date)
        c_matrix_next_shift = calculate_makespan(queue_next_day, machine_dict, workers_dict,
            date_next_shift, c_matrix[c_matrix.Duration.ne(timedelta(0))])
        return c_matrix_next_shift.loc[c_matrix_next_shift.Duration.ne(timedelta(0))]
    return c_matrix.loc[c_matrix.Duration.ne(timedelta(0))]

def fetch_machine_amount(machine_dict, start_date):
    machine_amount_day = machine_dict[start_date.date()]
    if start_date.time() < FIRST_SHIFT: return lib.get_column(2)(machine_amount_day)
    elif start_date.time() < SECOND_SHIFT: return lib.get_column(0)(machine_amount_day)
    elif start_date.time() < THIRD_SHIFT: return lib.get_column(1)(machine_amount_day)
    else: return lib.get_column(2)(machine_amount_day)

def get_fastest_machine(c_matrix, machine_id, machine_amount):
    fastest_machine = [max(c_matrix.loc[(slice(None), slice(None), machine_id, instance), 'End'])
        for instance in range(1, machine_amount + 1)]
    return min(fastest_machine), np.argmin(fastest_machine) + 1

def get_operation_details(c_matrix, job_id, machines, machine, machine_amount, workers_amount, operation_duration, start_date):
    # previous_machine = machines[machines.index(machine) - 1] if machines.index(machine) else 0
    previous_machine_duration = max(c_matrix.loc[(slice(None), job_id, machines), 'End']) #if previous_machine else start_date
    fastest_machine_duration, fastest_machine = get_fastest_machine(c_matrix, machine, machine_amount)
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
