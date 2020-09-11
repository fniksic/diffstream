from datetime import datetime
from os import path

import matplotlib.pyplot as plt
import numpy as np

SMALL_SIZE = 14
MEDIUM_SIZE = 16
BIGGER_SIZE = 18


def parse_memories(dir_name):
    with open(path.join(dir_name, "memory-log.txt")) as f:
        lines = f.readlines()
        memories = [parse_memory_line(line) for line in lines]
        return memories


def parse_memory_line(line):
    memory_string = line.split(":")[1].split("MB")[0]
    return int(memory_string)


def plot_memories_in_time(dir_name, mems_one, mems_two):
    size_one = len(mems_one)
    size_two = len(mems_two)

    width = 0.5

    x_one = np.linspace(0, size_one, size_one) - (width / 2)
    x_two = np.linspace(0, size_two, size_two) + (width / 2)

    fig, ax = plt.subplots()
    ax.bar(x_one, mems_one, width, label='With matcher')
    ax.bar(x_two, mems_two, width, label='Without matcher')
    ax.set_xticks([0, 1800, 3600, 5400, 7200])
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Used memory (MB)")
    ax.legend()

    plt.yscale('log')
    plt.tight_layout()
    # plt.show()
    plt.savefig(path.join(dir_name, "used_memory_in_time.pdf"))


def parse_unmatched(dir_name):
    with open(path.join(dir_name, "unmatched-items.txt")) as f:
        lines = f.readlines()
        unmatched = [parse_unmatched_line(line) for line in lines]
        return unmatched


def parse_unmatched_line(line):
    timestamp = datetime.strptime(line[0:26], "%Y-%m-%d %H:%M:%S,%f")
    split_line = line[27:].split(": ")
    unmatched_left = split_line[2].split(" ")[0]
    unmatched_right = split_line[3].split(" ")[0]
    total_processed = split_line[4].split(" ")[0]
    return timestamp, int(unmatched_left), int(unmatched_right), int(total_processed)


def process_unmatched(unmatched):
    timestamps = [(tup[0] - unmatched[0][0]).total_seconds() for tup in unmatched]
    left_unmatched = [tup[1] for tup in unmatched]
    right_unmatched = [tup[2] for tup in unmatched]
    total_unmatched = [tup[1] + tup[2] for tup in unmatched]
    avg_window = 1
    throughput = [(unmatched[i][3] - unmatched[max(0, i - avg_window)][3]) /
                  (timestamps[i] - timestamps[max(0, i - avg_window)])
                  for i in range(1, len(unmatched))]
    return timestamps, left_unmatched, right_unmatched, total_unmatched, throughput


def get_latencies(dir_name):
    latencies = np.fromfile(path.join(dir_name, 'durations-matcher-id-1.bin'), '>i4')
    return latencies


def plot_unmatched_in_time(dir_name, timestamps, left_unmatched, right_unmatched, total_unmatched):
    fig, ax = plt.subplots()
    #width = 0.3
    #ax.bar([ts - width for ts in timestamps], left_unmatched, width, label='Left')
    ax.bar(timestamps, total_unmatched, label='Total unmatched')
    #ax.bar([ts + width for ts in timestamps], right_unmatched, width, label='Right')
    ax.set_xticks([0, 1800, 3600, 5400, 7200])
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Number of unmatched events")
    # ax.legend()

    plt.tight_layout()
    # plt.show()
    plt.savefig(path.join(dir_name, "unmatched_in_time.pdf"))


def plot_unmatched_histogram(dir_name, total_unmatched):
    fig, ax = plt.subplots()
    ax.hist(total_unmatched, bins=100)
    ax.set_ylabel("Number of samples (taken every 1 second)")
    ax.set_xlabel("Number of unmatched events")

    plt.tight_layout()
    # plt.show()
    plt.savefig(path.join(dir_name, "unmatched_histogram.pdf"))


def plot_throughput(dir_name, ts_one, throughput_one, ts_two, throughput_two):
    fig, ax = plt.subplots()
    ax.plot(ts_one, throughput_one, label='With matcher', linewidth=0.5)
    ax.plot(ts_two, throughput_two, label='Without matcher', linewidth=0.5)
    ax.set_xticks([0, 1800, 3600, 5400, 7200])
    ax.set_ylabel('Throughput (events per second)')
    ax.set_xlabel('Time (seconds)')
    ax.legend()

    plt.tight_layout()
    plt.show()
    # plt.savefig(path.join(dir_name, 'throughput.pdf'))


def plot_latencies(dir_name, latencies_one, latencies_two):
    unique_lats_one, counts_one = np.unique(latencies_one, return_counts=True)
    np.cumsum(counts_one, out=counts_one)
    fractions_one = counts_one / float(len(latencies_one))

    unique_lats_two, counts_two = np.unique(latencies_two, return_counts=True)
    np.cumsum(counts_two, out=counts_two)
    fractions_two = counts_two / float(len(latencies_two))

    fix, ax = plt.subplots()
    ax.plot(unique_lats_one, fractions_one, label='With matcher')
    ax.plot(unique_lats_two, fractions_two, label='Without matcher')
    ax.set_xlabel('Latency (microseconds)')
    ax.set_ylabel('Cumulative fraction')
    ax.legend()

    plt.xscale('log')
    plt.tight_layout()
    # plt.show()
    plt.savefig(path.join(dir_name, 'latencies.pdf'))


# Yahoo benchmark on the server can run up to 40K input messages per
# second.  This is with setParallelism(2) and 1-2 implementations
# running at the same time. (It didn't really matter whether we
# execute one or two implementations at the same time)

# The matcher can handle 30K input messages per second (TODO: We have
# to make sure that indeed it handled it)

# TODO: Also execute a setParallelism(1) version of the two
# implementations, so that I find out what is the number of items
# that the implementations can handle with parallelism(1). If that is
# also close to 30K, then our matcher is not really slower than the
# implementation, and even though it has parallelism one it can
# handle almost as many messages as the parallelism(2)
# implementations. This ofcourse depends on the computation, but
# having a matcher that can handle what parallelism(2) can is a
# pretty good thing.

# A setParallelism(1) version can also barely handle 30K without falling behind.

def main():
    plt.rc('font', size=MEDIUM_SIZE)  # controls default text sizes
    plt.rc('axes', titlesize=BIGGER_SIZE)  # fontsize of the axes title
    plt.rc('axes', labelsize=MEDIUM_SIZE)  # fontsize of the x and y labels
    plt.rc('xtick', labelsize=MEDIUM_SIZE)  # fontsize of the tick labels
    plt.rc('ytick', labelsize=MEDIUM_SIZE)  # fontsize of the tick labels
    plt.rc('legend', fontsize=SMALL_SIZE)  # legend fontsize
    plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

    plt.rcParams['mathtext.fontset'] = 'stix'
    plt.rcParams['font.family'] = 'STIXGeneral'

    dir_one = "server_load_6000_time_300_test"
    dir_two = "server_load_6000_time_300_test"
    out_dir = "."

    # if(not len(sys.argv) == 2):
    #     print("Wrong arguments")
    #     print("You should call this as: `python3 collect_and_plot.py <results_dir>`")
    #     exit(1)
    #
    # dir_name = sys.argv[1]

    # mems_one = parse_memories(dir_one)
    # mems_two = parse_memories(dir_two)
    # plot_memories_in_time(out_dir, mems_one, mems_two)

    items_one = parse_unmatched(dir_one)
    ts_one, left_unmatched_one, right_unmatched_one, total_unmatched_one, throughput_one = process_unmatched(items_one)
    items_two = parse_unmatched(dir_two)
    ts_two, left_unmatched_two, right_unmatched_two, total_unmatched_two, throughput_two = process_unmatched(items_two)
    # plot_unmatched_in_time(out_dir, ts_one, left_unmatched_one, right_unmatched_one, total_unmatched_one)
    # plot_unmatched_histogram(out_dir, total_unmatched_one)
    plot_throughput(out_dir, ts_one[1:], throughput_one, ts_two[1:], throughput_two)

    # latencies_one = get_latencies(dir_one)
    # latencies_two = get_latencies(dir_two)
    # plot_latencies(out_dir, latencies_one, latencies_two)


if __name__ == '__main__':
    main()
