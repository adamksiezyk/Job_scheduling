import load_data
import Job_scheduling
import time
import numpy as np
import matplotlib.pyplot as plt


def run(amount):
    # Benchmark exec. time
    jobs, machines, start_date = load_data.load_data(
        'Linia_VA_2.xlsx', 'Linia VA', amount)
    # Load machines and workers dict
    machine_dict, workers_dict = Job_scheduling.parse_machines(machines)
    # Load jobs
    jobs = Job_scheduling.parse_jobs(jobs)
    t1 = time.time()
    Job_scheduling.schedule(
        jobs, machine_dict, workers_dict, start_date)
    t2 = time.time()
    return t2-t1


if __name__ == '__main__':
    # Benchmark
    amount = 20
    x = range(1, amount + 1)
    y = []
    for n in x:
        y.append(run(n))
        print(y)

    # y = [0.04584479331970215, 0.4348728656768799, 1.0350875854492188, 1.81174898147583, 2.802738904953003, 4.387550354003906, 6.591206312179565, 9.882599115371704, 14.086434125900269, 21.08683204650879, 29.609912157058716, 39.52410817146301, 49.904167890548706, 64.72859954833984, 88.04243230819702, 107.2513632774353, 137.24016094207764, 160.12968921661377, 196.33283877372742, 239.03350830078125, 280.6242392063141, 309.45441222190857, 343.1728506088257, 407.9153244495392, 479.13112020492554,
        #  548.3145234584808, 622.3714210987091, 702.0451014041901, 789.6337280273438, 892.3677945137024, 1030.5472979545593, 1164.0365974903107, 1283.297622203827, 1431.4992861747742, 1602.7985651493073, 1791.2816755771637, 1972.53928399086, 2170.349073410034, 2390.959001302719, 2622.291581630707, 2911.804224252701, 3256.3309656620026, 3421.8341104984283, 3757.9896850585938, 4274.725751876831, 4747.970718622208, 5272.017416238785, 6042.748406410217, 5660.265902519226, 6081.656017541885]

    # Regression
    b, a = np.polyfit(x, np.log(y), 1, w=np.sqrt(y))
    x2 = np.linspace(1, 2 * amount, 2 * amount)
    y2 = np.exp(a)*np.exp(b * x2)

    # Plot
    plt.plot(x, y, 'ro')
    plt.plot(x2, y2, 'b-')
    plt.show()
