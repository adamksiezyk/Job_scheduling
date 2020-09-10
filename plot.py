def plot(c_matrix):
    import matplotlib.pyplot as plt
    from datetime import timedelta
    filtered = c_matrix.loc[c_matrix['Duration'] != timedelta(0)]
    table = filtered[['Start', 'Duration']].xs(slice(None), level='Date')
    color = {'J1': "blue", 'J2': "red", 'J3': 'yellow'}
    fig, ax = plt.subplots()
    labels = []
    for i, (index, data) in enumerate(table.groupby(['Machine', 'Instance'])):
        labels.append(index)
        for (job_id, job_data) in data.groupby('Job'):
            d = job_data[['Start', 'Duration']]
            ax.broken_barh(d.values, (i - 0.4, 0.8), color=color[job_id])

    ax.grid(True)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlabel('time')
    plt.tight_layout()
    plt.show()