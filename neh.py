import functools
import numpy as np

def parse_duration(job_list):
    # TODO functional style
    duration_matrix = [[0] * len(job) for job in job_list]
    i = 0
    for job in job_list:
        for machine, duration in job.items():
            duration_matrix[i][int(machine[1:]) - 1] = duration
        i += 1
    return duration_matrix

def parse_order(job_list):
    return list(map(lambda job: list(map(lambda machine: int(machine[1:]) - 1, job.keys())), job_list))

def neh(job_list, machine_amount):
    # TODO functional style
    queue = list()
    # Duration matrix, rows - machines, columns - jobs
    duration_matrix = parse_duration(job_list)
    # Order matrix
    order_matrix = parse_order(job_list)
    # Sort the job list from longest to shortest duration
    jobs_sorted = sorted(range(len(job_list)), key=lambda i: calculate_job_makespan(job_list[i]), reverse=True)
    queue.append(jobs_sorted[0])
    for i in range(1, len(job_list)):
        c_min = float('inf')
        for j in range(i + 1):
            queue_tmp = insert(queue, j, jobs_sorted[i])
            c_matrix = calculate_makespan(queue_tmp, duration_matrix, order_matrix, machine_amount)
            c_tmp = max([max([max(machines) for machines in job]) for job in c_matrix])
            if c_min > c_tmp:
                queue_best = queue_tmp
                c_min = c_tmp
        queue = queue_best
    return queue, calculate_makespan(queue, duration_matrix, order_matrix, machine_amount)

# Duration of job over all machines
def calculate_job_makespan(job):
    return functools.reduce(lambda sum, key: sum + job[key], job, 0)

# Insert into immutable list
def insert(list, index, value):
    _list = list[:]
    _list.insert(index, value)
    return _list

# Calculate makespan of queue
def calculate_makespan(queue, duration_matrix, order_matrix, machine_amount):
    # TODO functional style
    c_matrix = [[[0] * amount for amount in machine_amount] for job in queue]

    # TODO: too complicated ...
    for job in queue:
        machine = order_matrix[job][0]
        operation_duration = duration_matrix[job][machine]
        if operation_duration == 0: continue
        previous_machine_duration = 0
        fastest_machine_duration = min([max([_job[machine][m] for _job in c_matrix]) for m in range(machine_amount[machine])])
        fastest_machine = np.argmin([max([_job[machine][m] for _job in c_matrix]) for m in range(machine_amount[machine])])
        c_matrix[job][machine][fastest_machine] = max(previous_machine_duration, fastest_machine_duration) + operation_duration
        for machine in order_matrix[job][1:]:
            operation_duration = duration_matrix[job][machine]
            if operation_duration == 0: continue
            previous_machine_duration = max([max(machine) for machine in c_matrix[job]])
            fastest_machine_duration = min([max([_job[machine][m] for _job in c_matrix]) for m in range(machine_amount[machine])])
            fastest_machine = np.argmin([max([_job[machine][m] for _job in c_matrix]) for m in range(machine_amount[machine])])
            c_matrix[job][machine][fastest_machine] = max(previous_machine_duration, fastest_machine_duration) + operation_duration
    return c_matrix