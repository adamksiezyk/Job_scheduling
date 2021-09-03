from itertools import groupby
from random import choices

import matplotlib.pyplot as plt
from src.model.entities.job import ScheduledJob


def gantt_chart(jobs: list[ScheduledJob]) -> None:
    """
    Shows a gantt chart of the scheduled jobs
    @param jobs: an ordered queue of scheduled jobs
    @return: None
    """
    machine_group = groupby(sorted(jobs, key=lambda j: j.machine_id), key=lambda j: j.machine_id)
    machine_dict = {k: [v for v in vl] for k, vl in machine_group}
    project_group = groupby(sorted(jobs, key=lambda j: j.project.id), key=lambda j: j.project.id)
    project_dict = {k: [v for v in vl] for k, vl in project_group}
    colors = {p_id: tuple(x / 10 for x in choices(range(10), k=3)) for p_id in project_dict.keys()}
    fig, gnt = plt.subplots()
    for m, jobs in enumerate(machine_dict.values()):
        for j in jobs:
            gnt.broken_barh([(j.start_dt, j.duration)], (m - 0.4, 0.8), color=colors[j.project.id], label=j.project.id)

    gnt.set_yticks(range(len(machine_dict.keys())))
    gnt.set_yticklabels(machine_dict.keys())
    gnt.invert_yaxis()
    gnt.set_ylabel("Machines")
    gnt.set_xlabel("Time")
    legend_without_duplicate_labels(gnt)
    plt.title("Scheduler")
    plt.show()


def legend_without_duplicate_labels(ax):
    handles, labels = ax.get_legend_handles_labels()
    unique = [(h, l) for i, (h, l) in enumerate(zip(handles, labels)) if l not in labels[:i]]
    ax.legend(*zip(*unique))
