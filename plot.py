def plot(c_matrix):
    import matplotlib.pyplot as plt
    import numpy as np
    from neh import next_shift
    from datetime import datetime, timedelta
    filtered = c_matrix.loc[c_matrix['Duration'] != timedelta(0)]
    table = filtered[['Start', 'Duration']].xs(slice(None), level='Date')
    color = {job_id: tuple(np.random.choice(np.arange(0, 1, 0.1), size=3)) for (job_id, _) in table.groupby('Job')}
    _, ax = plt.subplots()
    machines = []
    label_pool = []
    for i, (index, data) in enumerate(table.groupby(['Machine', 'Instance'])):
        machines.append(index)
        for (job_id, job_data) in data.groupby('Job'):
            d = job_data[['Start', 'Duration']]
            if job_id not in label_pool:
                ax.broken_barh(d.values, (i - 0.4, 0.8), color=color[job_id], label=job_id)
                label_pool.append(job_id)
            else:
                ax.broken_barh(d.values, (i - 0.4, 0.8), color=color[job_id])

    ax.set_yticks(range(len(machines)))
    ax.set_yticklabels(machines)
    ax.invert_yaxis()
    start = datetime.combine(c_matrix.index[0][0].date(), datetime.strptime('06:00', "%H:%M").time()) 
    end = next_shift(c_matrix.index[-1][0])
    ax.set_xticks(np.arange(start, end, timedelta(hours=8)), minor=True)
    ax.set_xlabel('time')
    ax.grid(which='minor', alpha=0.2)
    ax.grid(which='major', alpha=0.7)
    ax.legend()
    plt.title('Harmonogram')
    plt.show()