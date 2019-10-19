import matplotlib.pyplot as plt
import numpy as np

dir_name = "load_20000_time_600_leftpar_2_rightpar_2"


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

def plot_memories_in_time(memories):
    x = np.linspace(0, len(memories), len(memories))
    plt.plot(x, memories)
    plt.show()
    
def parse_unmatched(dir_name):
    with open(dir_name + "/unmatched-items.txt") as f:
        lines = f.readlines()
        # Drop the first line since it is a header
        no_header_lines = lines[1:]
        unmatched = [parse_unmatched_line(line) for line in no_header_lines]
        return unmatched

def parse_unmatched_line(line):
    unmatched_left = line.split(": ")[2].split(" ")[0]
    unmatched_right = line.split(": ")[3].rstrip()
    return (int(unmatched_left), int(unmatched_right))

def plot_unmatched_in_time(unmatched):
    x = np.linspace(0, len(unmatched), len(unmatched))
    left_right = list(zip(*unmatched))
    left = list(left_right[0])
    right = list(left_right[1])
    sums = [l + r for l, r in unmatched]
    print(left[:100])
    plt.plot(x, left)
    plt.plot(x, right)
    plt.plot(x, sums)
    plt.show()

def plot_unmatched_histogram(unmatched):
    left_right = list(zip(*unmatched))
    left = list(left_right[0])
    right = list(left_right[1])
    sums = [l + r for l, r in unmatched]
    
    n_bins = 20
    plt.hist(sums, bins=n_bins)
    plt.show()

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
    
# memories = parse_memories(dir_name)
# plot_memories_in_time(memories)

unmatched = parse_unmatched(dir_name)
# plot_unmatched_in_time(unmatched)
plot_unmatched_histogram(unmatched)
