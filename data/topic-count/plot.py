import matplotlib.pyplot as plt
import numpy as np

SMALL_SIZE = 10
MEDIUM_SIZE = 12
BIGGER_SIZE = 14

plt.rc('font', size=MEDIUM_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=BIGGER_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=MEDIUM_SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=MEDIUM_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

plt.rcParams['mathtext.fontset'] = 'stix'
plt.rcParams['font.family'] = 'STIXGeneral'

file_name = "results.txt"


def parse_exec_times(file_name):
    with open(file_name) as f:
        lines = f.readlines()
        exec_times = [parse_exec_time_line(line) for line in lines]
        return exec_times

def parse_exec_time_line(line):
    exec_time_string = line.split(": ")[1].split(" ")[0]
    return (int(exec_time_string) / 1000)

def plot_execution_times(exec_times):
    fig = plt.figure()
    x = np.linspace(1, len(exec_times), len(exec_times))
    plt.plot(x, exec_times)
    plt.title("Execution time with different parallelism levels")
    plt.xlabel("Flink Parallelism")
    plt.ylabel("Execution Time (seconds)")
    # plt.show()
    plt.savefig("topic_counts_execution_times.pdf")
    

exec_times = parse_exec_times(file_name)
print(exec_times)
plot_execution_times(exec_times)
