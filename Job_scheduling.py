import time
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

# Parse loaded JSON machine string to desired format


def parse_machines(machine_dict):
    # machine_string_format = machine_string.replace("'", '"')
    # machine_dict = json.loads(machine_string_format)
    machine_amount = {date:
                      {machine: [0 if shift == 'x' else int(shift.split('x')[0]) for shift in machine_dict[date][machine]]
                       for machine in machine_dict[date]}
                      for date in machine_dict}
    workers_amount = {date:
                      {machine: [0 if shift == 'x' else float(shift.split('x')[1]) for shift in machine_dict[date][machine]]
                       for machine in machine_dict[date]}
                      for date in machine_dict}
    return machine_amount, workers_amount

# Parse loaded jobs data structure to desired format


def parse_jobs(jobs):
    if isinstance(jobs, list):
        return functools.reduce(
            lambda ans, job: lib.append(ans, parse_jobs(job)),
            jobs,
            [])
    elif jobs == '>' or jobs == '&':
        return jobs
    elif isinstance(jobs, dict):
        return {(job_id, job_start): parse_machines_in_job(machines)
                for (job_id, job_start), machines in jobs.items()}
    else:
        raise RuntimeError('Wrong jobs format')

# Helfer functioin for parsing machines in jobs


def parse_machines_in_job(machines):
    if isinstance(machines, list):
        return functools.reduce(
            lambda ans, machine: lib.append(
                ans, parse_machines_in_job(machine)),
            machines,
            [])
    elif machines == '&' or machines == '>':
        return machines
    elif isinstance(machines, dict):
        return {machine: (timedelta(hours=time), delay)
                for machine, (time, delay) in machines.items()}
    else:
        raise RuntimeError('Wrong machines format')

# Schedule jobs


def schedule(job, machine_dict, workers_dict, start_date, c_matrix_old=pd.DataFrame()):
    if isinstance(job, dict):
        return neh.neh(job, machine_dict, workers_dict, start_date, c_matrix_old)
    elif isinstance(job, list):
        if job[1] == '&':
            # TODO we assume the two jobs don't use the same machines!
            result_0 = schedule(job[0], machine_dict, workers_dict, start_date)
            result_2 = schedule(job[2], machine_dict,
                                workers_dict, start_date, result_0[2])
            c_matrix = result_2[2]
            return lib.append(result_0[0], result_2[0]), max(result_0[1], result_2[1]), c_matrix
        elif job[1] == '>':
            result_0 = schedule(job[0], machine_dict, workers_dict, start_date)
            result_2 = schedule(
                job[2], machine_dict, workers_dict, start_date + result_0[1], result_0[2])
            c_matrix = result_2[2]
            return lib.append(result_0[0], result_2[0]), result_0[1] + result_2[1], c_matrix
    else:
        raise RuntimeError('Wrong jobs format.')


if __name__ == '__main__':
    jobs, resources = load_data.load_data(
        'Linia_VA.xlsx', 'Linia VA', 'WorkCalendar.xlsx', 'Sheet1', 5)

    # Start date
    start_date = min(jobs.keys(), key=lambda key: key[1])[1]
    # Load machines and workers dict
    machine_dict, workers_dict = parse_machines(resources)
    # Load jobs
    jobs = parse_jobs(jobs)
    # Benchmark exec. time
    t1 = time.time()
    # Schedule jobs
    queue, duration, c_matrix = schedule(
        jobs, machine_dict, workers_dict, start_date)
    t2 = time.time()
    # Print optimal queue
    # print('\n', queue, '\n')
    # Print whole project duration
    print(duration, '\n')
    # Display the c_matrix
    # with pd.option_context('display.max_rows', None):  # more options can be specified also
    # print(c_matrix)
    c_matrix.to_excel("output.xlsx")
    # Plot the schedule
    plot.plot(c_matrix)
    # Display exec. time
    print('\n', t2-t1)
