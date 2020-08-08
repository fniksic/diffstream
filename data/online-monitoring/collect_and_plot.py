import matplotlib.pyplot as plt
import numpy as np
import sys

SMALL_SIZE = 14
MEDIUM_SIZE = 16
BIGGER_SIZE = 18

plt.rc('font', size=MEDIUM_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=BIGGER_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=MEDIUM_SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=MEDIUM_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

plt.rcParams['mathtext.fontset'] = 'stix'
plt.rcParams['font.family'] = 'STIXGeneral'

# dir_name = "load_20000_time_600_leftpar_2_rightpar_2/" 
# dir_name = "server_load_30000_time_3600_leftpar_2_rightpar_2/"
# dir_name = "server_debug_memory/"
dir_name = "server_load_30000_time_7200_leftpar_2_rightpar_2/"

if(not len(sys.argv) == 2):
    print("Wrong arguments")
    print("You should call this as: `python3 collect_and_plot.py <results_dir>`")
    exit(1)

dir_name = sys.argv[1]

def parse_memories(dir_name):
    with open(dir_name + "/memory-log.txt") as f:
        lines = f.readlines()
        # Drop the first line since it is a header
        no_header_lines = lines[1:]
        memories = [parse_memory_line(line) for line in no_header_lines]
        return memories

def parse_memory_line(line):
    memory_string = line.split(":")[1].split("MB")[0]
    return int(memory_string)

def plot_memories_in_time(dir_name, memories):
    fig = plt.figure()
    x = np.linspace(0, len(memories), len(memories))
    plt.plot(x, memories)
    # plt.title("Memory Usage")
    plt.xlabel("Time (seconds)")
    plt.ylabel("Used Memory (MB)")
    # plt.show()
    plt.savefig(dir_name + "/used_memory_in_time.pdf")
    
def parse_unmatched(dir_name):
    with open(dir_name + "/unmatched-items.txt") as f:
        lines = f.readlines()
        # Drop the first line since it is a header
        no_header_lines = lines[1:]
        unmatched = [parse_unmatched_line(line) for line in no_header_lines]
        return unmatched

def parse_unmatched_line(line):
    unmatched_left = line.split(": ")[2].split(" ")[0]
    unmatched_right = line.split(": ")[3].split(" ")[0].rstrip()
    return (int(unmatched_left), int(unmatched_right))

def plot_unmatched_in_time(dir_name, unmatched):
    fig = plt.figure()
    x = np.linspace(0, len(unmatched), len(unmatched))
    left_right = list(zip(*unmatched))
    left = list(left_right[0])
    right = list(left_right[1])
    sums = [l + r for l, r in unmatched]
    print(left[:100])
    plt.plot(x, left, label='Left')
    plt.plot(x, right, label='Right')
    plt.plot(x, sums, label='Total')
    # plt.title("Unmatched items in time")
    plt.xlabel("Time (seconds)")
    plt.ylabel("Number of unmatched items")
    plt.legend()
    # plt.show()
    plt.savefig(dir_name + "/unmatched_in_time.pdf")

def plot_unmatched_histogram(dir_name, unmatched):
    fig = plt.figure()
    left_right = list(zip(*unmatched))
    left = list(left_right[0])
    right = list(left_right[1])
    sums = [l + r for l, r in unmatched]
    
    n_bins = 20
    plt.hist(sums, bins=n_bins)
    # plt.title("Histogram of total unmatched items")
    plt.ylabel("Number of samples (taken every 1 second)")
    plt.xlabel("Number of unmatched items")
    # plt.show()
    plt.savefig(dir_name + "/unmatched_histogram.pdf")


## Yahoo benchmark on the server can run up to 40K input messages per
## second.  This is with setParallelism(2) and 1-2 implementations
## running at the same time. (It didn't really matter whether we
## execute one or two implementations at the same time)

## The matcher can handle 30K input messages per second (TODO: We have
## to make sure that indeed it handled it)

## TODO: Also execute a setParallelism(1) version of the two
## implementations, so that I find out what is the number of items
## that the implementations can handle with parallelism(1). If that is
## also close to 30K, then our matcher is not really slower than the
## implementation, and even though it has parallelism one it can
## handle almost as many messages as the parallelism(2)
## implementations. This ofcourse depends on the computation, but
## having a matcher that can handle what parallelism(2) can is a
## pretty good thing.

## A setParallelism(1) version can also barely handle 30K without falling behind.
    
memories = parse_memories(dir_name)
plot_memories_in_time(dir_name, memories)

unmatched = parse_unmatched(dir_name)
plot_unmatched_in_time(dir_name, unmatched)
plot_unmatched_histogram(dir_name, unmatched)
