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


def plot_memory_in_time(dir_name, mems_one, mems_two):
    size_one = len(mems_one)
    size_two = len(mems_two)

    x_one = np.linspace(0, size_one, size_one)
    x_two = np.linspace(0, size_two, size_two)

    fig, ax = plt.subplots()
    ax.scatter(x_one, mems_one, 0.2, label='With matcher')
    ax.scatter(x_two, mems_two, 0.2, label='Without matcher', alpha=0.7)
    ax.set_xticks([0, 1800, 3600, 5400, 7200])
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Used memory (MB)")
    ax.legend(markerscale=10)

    plt.tight_layout()
    # plt.show()
    plt.savefig(path.join(dir_name, "used_memory_in_time.pdf"))


def plot_memory_ccdfs(dir_name, mems_one, mems_two):
    unique_mems_one, counts_one = np.unique(mems_one, return_counts=True)
    np.cumsum(counts_one, out=counts_one)
    fraction_one = 1.0 - counts_one / float(len(mems_one))

    unique_mems_two, counts_two = np.unique(mems_two, return_counts=True)
    np.cumsum(counts_two, out=counts_two)
    fraction_two = 1.0 - counts_two / float(len(mems_two))

    fig, ax = plt.subplots()
    ax.plot(unique_mems_one, fraction_one, label='With matcher')
    ax.plot(unique_mems_two, fraction_two, label='Without matcher', alpha=0.7)
    ax.set_xlabel('Used memory (MB)')
    ax.set_ylabel('Complementary cumulative fraction')
    ax.legend()
    plt.yscale('log')

    plt.tight_layout()
    # plt.show()
    plt.savefig(path.join(dir_name, 'memory_ccdf.pdf'))


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
    cut_point = next(i for i in reversed(range(len(unmatched))) if unmatched[i][3] != unmatched[len(unmatched) - 1][3])
    unmatched = unmatched[0:cut_point]
    timestamps = [(tup[0] - unmatched[0][0]).total_seconds() for tup in unmatched]
    total_unmatched = [tup[1] + tup[2] for tup in unmatched]
    throughput = [(unmatched[i][3] - unmatched[i - 1][3]) /
                  (timestamps[i] - timestamps[i - 1])
                  for i in range(1, len(unmatched))]
    avg_window = 60
    throughput_avg = [(unmatched[i][3] - unmatched[max(0, i - avg_window)][3]) /
                  (timestamps[i] - timestamps[max(0, i - avg_window)])
                  for i in range(1, len(unmatched))]
    return timestamps, total_unmatched, throughput, throughput_avg


def get_latencies(dir_name):
    latencies = np.fromfile(path.join(dir_name, 'durations-matcher-id-1.bin'), '>i4')
    return latencies


def plot_unmatched_in_time(dir_name, timestamps, total_unmatched):
    fig, ax = plt.subplots()
    ax.scatter(timestamps, total_unmatched, 0.2, label='Total unmatched')
    ax.set_xticks([0, 1800, 3600, 5400, 7200])
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Number of unmatched events")

    plt.tight_layout()
    # plt.show()
    plt.savefig(path.join(dir_name, "unmatched_in_time.pdf"))


def plot_unmatched_histogram(dir_name, total_unmatched):
    fig, ax = plt.subplots()
    ax.hist(total_unmatched, bins=100)
    ax.set_ylabel("Number of samples (taken every 1 second)")
    ax.set_xlabel("Number of unmatched events")
    plt.show()
    # plt.savefig(path.join(dir_name, "unmatched_histogram.pdf"))


def plot_unmatched_ccdf(dir_name, total_unmatched):
    unique_unmatched, counts = np.unique(total_unmatched, return_counts=True)
    np.cumsum(counts, out=counts)
    fractions = 1.0 - counts / float(len(total_unmatched))

    fig, ax = plt.subplots()
    ax.plot(unique_unmatched, fractions)
    ax.set_xlabel('Number of unmatched events')
    ax.set_ylabel('Complementary cumulative fraction')
    plt.yscale('log')

    plt.tight_layout()
    # plt.show()
    plt.savefig(path.join(dir_name, 'unmatched_ccdf.pdf'))


def plot_throughput(dir_name, ts_one, throughput_one, throughput_avg_one, ts_two, throughput_two, throughput_avg_two):
    fig, ax = plt.subplots()
    # ax.plot(ts_one, throughput_one, label='With matcher', linewidth=0.2)
    # ax.plot(ts_two, throughput_two, label='Without matcher', linewidth=0.2)
    ax.plot(ts_one, throughput_avg_one, label='With matcher', linewidth=0.2)
    ax.plot(ts_two, throughput_avg_two, label='Without matcher', linewidth=0.2, alpha=0.7)
    # ax.vlines(470, 0, 42000, linewidth=0.5, colors='gray')
    # ax.set_xticks([0, 470, 1800, 3600, 5400, 7200])
    ax.vlines([1737, 2023], 0, 42000, linewidth=0.5, colors='gray')
    ax.set_xticks([0, 625, 1250, 1737, 2023, 2500])
    ax.set_ylabel('Throughput (events/s)')
    ax.set_xlabel('Time (seconds)')
    ax.legend()

    plt.tight_layout()
    # plt.show()
    plt.savefig(path.join(dir_name, 'throughput-accelerated.pdf'))


def plot_latencies(dir_name, latencies):
    unique_lats, counts = np.unique(latencies, return_counts=True)
    np.cumsum(counts, out=counts)
    fractions = 1.0 - counts / float(len(latencies))

    fix, ax = plt.subplots()
    ax.plot(unique_lats, fractions)
    ax.set_xlabel('Latency (microseconds)')
    ax.set_ylabel('Complementary cumulative fraction')
    plt.xscale('log')
    plt.yscale('log')

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
    plt.rcParams['pdf.fonttype'] = 42

    out_dir = "."

    # Plot accelerated throughput

    dir_one = "server_load_40000_accel_10_time_2500_real_throughput"
    dir_two = "server_load_40000_accel_10_time_2500_dummy_throughput"

    items_one = parse_unmatched(dir_one)
    ts_one, total_unmatched_one, throughput_one, throughput_avg_one = process_unmatched(items_one)
    items_two = parse_unmatched(dir_two)
    ts_two, total_unmatched_two, throughput_two, throughput_avg_two = process_unmatched(items_two)
    plot_throughput(out_dir, ts_one[1:], throughput_one, throughput_avg_one, ts_two[1:], throughput_two, throughput_avg_two)

    # Plot memory in time, memory ccdf, unmatched items in time, unmatched items ccdf, and latency ccdf

    dir_one = "server_load_45000_accel_0_time_7200_real_lats"
    dir_two = "server_load_45000_accel_0_time_7200_dummy_lats2"

    items_one = parse_unmatched(dir_one)
    ts_one, total_unmatched_one, throughput_one, throughput_avg_one = process_unmatched(items_one)

    mems_one = parse_memories(dir_one)
    mems_two = parse_memories(dir_two)
    plot_memory_ccdfs(out_dir, mems_one, mems_two)
    plot_memory_in_time(out_dir, mems_one, mems_two)
    plot_unmatched_in_time(out_dir, ts_one, total_unmatched_one)
    plot_unmatched_ccdf(out_dir, total_unmatched_one)
    # plot_unmatched_histogram(out_dir, total_unmatched_one)

    latencies = get_latencies(dir_one)
    plot_latencies(out_dir, latencies)


if __name__ == '__main__':
    main()
