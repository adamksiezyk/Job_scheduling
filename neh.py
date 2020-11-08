import lib
import functools
import concurrent.futures
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
    # TODO sort from fastest start
    # Sort the job list from longest to shortest duration
    jobs_sorted = sorted(
        job_list.items(), key=lambda job: job[0][1], reverse=True)
    queue.append(jobs_sorted[0])
    for i in range(1, len(job_list)):
        # Run each permutation on different thread
        with concurrent.futures.ProcessPoolExecutor() as executor:
            processes = [executor.submit(calculate_queue, queue, jobs_sorted[i], j, machine_dict,
                                         workers_dict, start_date, c_matrix_old) for j in range(i + 1)]
            results = [process.result()
                       for process in concurrent.futures.as_completed(processes)]
        queue = min(results, key=lambda result: result[0])[1]
    c_matrix = calculate_makespan(
        queue, machine_dict, workers_dict, start_date, c_matrix_old)
    c_max = get_c_max(start_date, c_matrix)
    return queue, c_max, c_matrix

# Calculate makespan of queue


def calculate_queue(queue, job, j, machine_dict, workers_dict, start_date, c_matrix_old):
    queue_tmp = lib.insert(queue, j, job)
    c_matrix = calculate_makespan(
        queue_tmp, machine_dict, workers_dict, start_date, c_matrix_old)
    return get_c_max(start_date, c_matrix), queue_tmp


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
    # Index
    instances = lib.flatten([list(range(1, amount + 1))
                             for amount in machine_amount.values()])
    machines = lib.flatten([[machine] * machine_amount[machine]
                            for machine in machine_amount])
    jobs = lib.flatten([[job_id] * len(instances)
                        for (job_id, _), __ in queue])
    arrays = [[start_date] * len(jobs), jobs,
              machines * len(queue), instances * len(queue)]
    index = pd.MultiIndex.from_arrays(
        arrays=arrays, names=['Date', 'Job', 'Machine', 'Instance'])
    # Blank matrix
    c_matrix_init = pd.DataFrame(
        data=[[start_date] * 2] * len(jobs), index=index, columns=['Start', 'End'])
    c_matrix_init['Duration'] = c_matrix_init['End'] - c_matrix_init['Start']
    c_matrix = c_matrix_old.append(c_matrix_init).sort_index()

    # TODO: too complicated ...
    for (job_id, job_start_date), job in queue:
        start_date = job_start_date if job_start_date > start_date else start_date
        if isinstance(job, dict):
            for machine in job:
                operation_duration = job[machine][0]
                delay = job[machine][1]
                if not operation_duration:
                    continue
                if not machine_amount[machine] or not workers_amount[machine]:
                    job_next_day = lib.slice_dict(job, machine)[1]
                    queue_next_day.append((job_id, job_next_day))
                    break
                operation_start, operation_end, fastest_machine = get_operation_details(
                    c_matrix, job_id, list(
                        job.keys()), machine, machine_amount[machine],
                    workers_amount[machine], operation_duration, delay)
                if operation_end > next_shift(start_date):
                    job_next_day = lib.slice_dict(job, machine)[1]
                    if operation_start < next_shift(start_date):
                        c_matrix.loc[(start_date, job_id, machine,
                                      fastest_machine), 'Start'] = operation_start
                        c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'End'] = next_shift(
                            start_date)
                        c_matrix.loc[(start_date, job_id, machine, fastest_machine), 'Duration'] = next_shift(
                            start_date) - operation_start
                        job_next_day[machine] = ((
                            operation_end - next_shift(start_date)) * workers_amount[machine], '0d')
                    job_next_day[machine] = (job_next_day[machine][0], '0d')
                    queue_next_day.append(
                        ((job_id, operation_start), job_next_day))
                    break
                c_matrix.loc[(start_date, job_id, machine,
                              fastest_machine), 'Start'] = operation_start
                c_matrix.loc[(start_date, job_id, machine,
                              fastest_machine), 'End'] = operation_end
                c_matrix.loc[(start_date, job_id, machine, fastest_machine),
                             'Duration'] = operation_end - operation_start

        elif isinstance(job, list):
            if job[1] == '&':
                result_0 = calculate_makespan(
                    [((job_id, job_start_date), job[0])
                     ], machine_dict, workers_dict, start_date,
                    c_matrix.loc[c_matrix.Duration.ne(timedelta(0))])
                result_2 = calculate_makespan(
                    [((job_id, job_start_date), job[2])], machine_dict, workers_dict, start_date, result_0)
                c_matrix = result_2
            elif job[1] == '>':
                result_0 = calculate_makespan(
                    [((job_id, job_start_date), job[0])
                     ], machine_dict, workers_dict, start_date,
                    c_matrix.loc[c_matrix.Duration.ne(timedelta(0))])
                end_date = start_date if result_0.empty or job_id not in result_0.index.get_level_values(
                    'Job') else max(result_0.loc[(slice(None), job_id), 'End'])
                result_2 = calculate_makespan(
                    [((job_id, job_start_date), job[2])], machine_dict, workers_dict, end_date, result_0)
                c_matrix = result_2
        else:
            raise RuntimeError('Wrong jobs format.')

    if queue_next_day:
        date_next_shift = next_shift(start_date)
        c_matrix_next_shift = calculate_makespan(queue_next_day, machine_dict, workers_dict,
                                                 date_next_shift, c_matrix.loc[c_matrix.Duration.ne(timedelta(0))])
        return c_matrix_next_shift.loc[c_matrix_next_shift.Duration.ne(timedelta(0))]
    return c_matrix.loc[c_matrix.Duration.ne(timedelta(0))]


def fetch_machine_amount(machine_dict, start_date):
    machine_amount_day = machine_dict[start_date.date()]
    if start_date.time() < FIRST_SHIFT:
        return lib.get_column(2)(machine_amount_day)
    elif start_date.time() < SECOND_SHIFT:
        return lib.get_column(0)(machine_amount_day)
    elif start_date.time() < THIRD_SHIFT:
        return lib.get_column(1)(machine_amount_day)
    else:
        return lib.get_column(2)(machine_amount_day)


def get_fastest_machine(c_matrix, machine_id, machine_amount):
    fastest_machine = [max(c_matrix.loc[(slice(None), slice(None), machine_id, instance), 'End'])
                       for instance in range(1, machine_amount + 1)]
    return min(fastest_machine), np.argmin(fastest_machine) + 1


def get_operation_details(c_matrix, job_id, machines, machine, machine_amount, workers_amount, operation_duration, delay):
    previous_machine_duration = max(
        c_matrix.loc[(slice(None), job_id, machines), 'End'])
    fastest_machine_duration, fastest_machine = get_fastest_machine(
        c_matrix, machine, machine_amount)
    operation_start = parse_delay(max(previous_machine_duration, fastest_machine_duration), delay)
    operation_end = operation_start + operation_duration / workers_amount
    return operation_start, operation_end, fastest_machine


def get_c_max(start_date, c_matrix):
    return pd.Timedelta(0) if c_matrix.empty else max(c_matrix['End']) - start_date


def next_shift(start_date):
    if start_date.time() < FIRST_SHIFT:
        return datetime.combine(start_date.date(), FIRST_SHIFT)
    elif start_date.time() < SECOND_SHIFT:
        return datetime.combine(start_date.date(), SECOND_SHIFT)
    elif start_date.time() < THIRD_SHIFT:
        return datetime.combine(start_date.date(), THIRD_SHIFT)
    else:
        return datetime.combine(start_date.date() + timedelta(days=1), FIRST_SHIFT)

def parse_delay(date, delay):
    if delay[-1].lower() == 'd':
        return lib.next_n_days(date, int(delay[:-1]))
    else:
        return lib.next_weekday(date, 0)