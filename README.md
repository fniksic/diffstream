# DiffStream

A differential testing library for [Apache Flink](https://flink.apache.org/) programs.

The tool and the underlying methodology are described in the OOPSLA paper, which can be found in `PAPER.pdf`.
This README contains instructions on how to quickly get started, how to use the tool, and how to run the experiments from the paper.

## Getting Started

**Virtual Machine:** The virtual machine should come with all dependencies installed.
(For later users, the installation instructions can be found in `INSTALL.md`.)

    Username: **diffstream**
    Password: **diffstream**

DiffStream is a testing tool. The tool is used by writing two Flink programs (using the Java API), providing a specification of correct ordering behavior, and then connecting the output to the DiffStream matcher. DiffStream either succeeds (normal termination) or reports a bug (raises StreamsNotEquivalentException). For more details on how to use it yourself or modify the existing examples, see the (optional) "Tutorial" below.

To check that the tool is working properly, you can run `mvn test` (in the top-level directory, where this README is). This runs unit tests.

To quickly validate the paper experiments, we provide the following shell scripts:

- (Section 5.1 case study)

- (Section 5.2 case study)

- (Section 5.3 case study) Run `./run_mapreduce.sh`. Verify that the 12 test results pass and the script says BUILD SUCCEEDED. (Each checkmark in the Section 5.3 table corresponds to one test result, except the final row, StrConcat, where the two checkmarks correspond to 4 tests.) Please note that because these are random tests, it is possible (though unlikely) that a test may not detect the bug, and the script will fail. If so, run the script again.

- (Section 5.4 case study) Run `./run_online_monitoring.sh -l 1000 -t 120` and after it finishes, check that 3 plots have been produced in `data/online-monitoring//server_load_1000_time_120_leftpar_1_right_par_2`.

More detail on each of these case studies can be found under "Running the Experiments".

## Tutorial

## Running the Experiments

### 5.1 Taxi Distance

The source code of the Taxi Distance experiment is located in
`examples/taxi`. To run all the TaxiDistance tests, you can just use
the script `./run_taxi.sh`, but we suggest using IntelliJ to open the
tests described below one by one, and running them by clicking the
green arrow next to their definitions in IntelliJ. The wrong parallel
implementation is caught by two tests (`testPositionsByKey`,
`testPositionsByKeyInputGenerator`) in file
`examples/taxi/src/test/java/edu/upenn/diffstream/examples/taxi/KeyByParallelismTest.java`,
the first of which uses manual input, while the second uses random
input generation. The correct parallel implementation is tested
against the sequential one in
`correctTestPositionsByKeyInputGenerator` in file
`examples/taxi/src/test/java/edu/upenn/diffstream/examples/taxi/KeyByParallelismTest.java`. All
three of these tests follow the structure shown in Figure 10 of the
paper.

The manual implementation of the matcher (described in Section 5.1)
can be seen in two files
(`examples/taxi/src/main/java/edu/upenn/diffstream/examples/taxi/KeyByParallelismManualMatcher.java`,
`examples/taxi/src/main/java/edu/upenn/diffstream/examples/taxi/KeyByParallelismManualSink.java`). The
test that uses the manual matcher is named `manualTestPositionsByKey`
and is in file
`examples/taxi/src/test/java/edu/upenn/diffstream/examples/taxi/KeyByParallelismTest.java`.

### 5.2 Topic Count

The source code of the Topic Count case study (Section 5.2 in the paper) is located in the directory `diffstream/examples/topiccount`. Make sure the code is compiled by running

```
mvn package
```

Recall from the paper that the goal in this case study is to implement a streaming program for finding the most frequent word topic in a document. Documents are given as a stream of words delineated with end-of-file markers, and the mapping from words to topics is stored in an external Redis database. As explained in the paper, there is a fairly straightforward sequential solution and a tricky-to-implement parallel solution to this problem. The two solutions are located in files `src/main/java/edu/upenn/diffstream/examples/topiccount/TopicCountSequential.java` and `src/main/java/edu/upenn/diffstream/examples/topiccount/TopicCountParallel.java`. The first step of the case study is to look at the code and convince yourself that the parallel version looks dauntingly tricky and quite different then the sequential version.

We have prepared scripts to help run the programs. The scripts are located in `diffstream/examples/topiccount`.
`prepare-topiccount.sh` starts Flink and Redis and populates the word-to-topic mapping if needed.
`run-topiccount.sh` runs the two versions of the program and the related differential tests.
`teardown-topiccount.sh` stops Flink and Redis.

Try to run both the sequential and the parallel versions:

```
./prepare-topiccount.sh
./run-topiccount.sh sequential
./run-topiccount.sh parallel
```

By default, the programs are run on a stream of 10 documents, each document containing 100,000 random words. The default parallelism for the parallel program is 4. These values can be changed by passing additional arguments to `run-topiccount.sh`, for example,

```
./run-topiccount.sh parallel --totalDocuments 5 --wordsPerDocument 20000 --parallelism 3
```

will run the parallel program on 5 documents, each containing 20,000 words, and with parallelism 3. The output of the programs—the list of most frequent topics for each document—will appear in a text file called `topics.txt`.

The next step in the study is to make sure that the sequential program cannot be parallelized simply by increasing the parallelism to more than 1. To do this, we have prepared a differential test (`src/main/java/edu/upenn/diffstream/examples/topiccount/TopicCountSequentialDiffTesting.java`). To run the test, execute

```
./run-topiccount.sh seq-diff-test --totalDocuments 1 --wordsPerDocument 1
```

Note that we don’t need long documents to show the non-equivalence: a single document with a single word will suffice, as parallelizing the sequential program is outright wrong. The test should exit by saying “Streams are NOT equivalent!”

However, the correct parallel program should be equivalent to the (non-parallelized) sequential program. To ensure this is the case, we have prepared another differential test (`src/main/java/edu/upenn/diffstream/examples/topiccount/TopicCountParallelDiffTesting.java`). To run the test, execute

```
./run-topiccount.sh par-diff-test
```

The test should exit by saying “Streams are equivalent!”

The last step of the study is to make sure that there is a performance benefit in parallelizing the computation. To test this, execute

```
./run-topiccount.sh par-scaleup
```

The script will execute the parallel Topic Count with parallelism increasing from 1 to 8 and produce a summary approximately like the following.

```
parallelism    time (ms)    speedup
1   	 73637   	 1.00
2   	 51179   	 1.43
3   	 31807   	 2.31
4   	 30653   	 2.40
5   	 31666   	 2.32
6   	 31025   	 2.37
7   	 32793   	 2.24
8   	 28417   	 2.59
```

The results reported in Table 1 in the paper show a speedup by a factor of 5 on 5 documents containing 500,000 words, but the original experiment was done on a server with more processor cores than what is available in the virtual machine.

At the end of the study, you can shut down Flink and Redis by executing

```
./teardown-topiccount.sh
```

### 5.3 MapReduce

The source code of the MapReduce case study (Section 5.3 in the paper) is located in the directory `examples/mapreduce`.

To simply run all the experiments, you can use the script `./run_mapreduce.sh`. This should print a bunch of tests, with the expected and actual results. Each result corresponds to a checkmark in the table in Section 5.3 (this is reflected in the test name, though due to details of Java JUnit, the tests run out of order from what is in the table). StrConcat has 4 tests instead of just 2 for the 2 checkmarks, because we explored two different ways to correct for nondeterminism, as described in the paper text. If any results differ from what is expected, the build will fail. Otherwise it should show that 12 tests are run with BUILD SUCCEEDED.

Please note that due to random input generation as well as nondeterminism of parallel Flink programs, it is possible (though unlikely) that one of the tests will get an input that does not expose the bug in the given program. If this happens, just re-run the tests.

Please also note in the shell script that we suppress "WARNING:" statements output by `mvn test`. These are warnings unrelated to DiffStream and just related to certain dependencies, which we clean up so that the output is easier to read.

In more detail:

- The experiment consists of 12 tests, implemented in `examples/mapreduce/src/test/java/edu/upenn/diffstream/examples/mapreduce/MapReduceNondeterminismTest.java`.

- The flink programs under test here are MapReduce reducers, adapted to the streaming setting. The implementations of the five reducers under test can be found in `examples/mapreduce/src/main/java/edu/upenn/diffstream/examples/mapreduce/reducers`.
Each test (`@Test` in the source file mentioned in the first bullet) looks at one particular reducer under some particular input conditions and given a test specification ("dependence relation"). A sequential and parallel instance of the reducer are set up and run in Flink, and the outputs they produce are compared (differential testing). The test then reports:

  - If the two programs produced equivalent results, up to the specification, the test shows that the streams were found to be equivalent.

  - If the two programs produced inequivalent results, up to the specification, the test shows that the streams differed.

  In this case, because we are looking at MapReduce programs, "differ" means that the reducer in question is nondeterministic.  And "equivalent" means that the reducer appears to be deterministic on the given input in question (at least in this run). Whether it is deterministic or not does depend on the input conditions, which is what this case study tests. Most reducers are nondeterministic for all inputs (column 1 in the table in the paper), but deterministic under well-formed input (column 2 in the table).

  StrConcat is a special case: here the program is nondeterministic, but this is allowed by the application requirements (column 3 in the table); so the StrConcat tests are showing how to avoid flagging the nondeterminism as a bug by setting up a DiffStream test in a particular way, or by re-implementing the reducer.

  The Xs in the table in the paper don't correspond to any tests here. Instead, X indicates that DiffStream can't be used to get achieve the desired result. That is, we do not know of a way to write the test which avoids a false positive for that scenario.

- Three auxiliary files are in `examples/mapreduce/src/main/java/edu/upenn/diffstream/examples/mapreduce/reducers`: `BadDataSource.java` and `GoodDataSource.java` are to generate random input, and `ReducerExamplesItem.java` describes the type of events in the input stream for this particular example.

If you like, here are some other things you can try:

- You can view the actual output streams that are being compared by DiffStream to detect bugs, rather than just the results of each test. To do so, change the line `private static final boolean DEBUG = false` to `private static final boolean DEBUG = true` in `MapReduceNondeterminismTest.java`.

- In case you want to run a specific test alone (or modify it and see that it fails), find the test in `MapReduceNondeterminismTest.java` (under `@Test`), and take note of its name. Make sure you are in the `examples/mapreduce` subdirectory, then run `mvn test -Dtest=<Test Name>`. For example:

  ```
  mvn test -Dtest=MapReduceNondeterminismTest#testIndexValuePairIncorrect
  ```

  You can also add ` |& grep -v "WARNING:"` to suppress the irrelevant warnings in the output.

### 5.4 Online Monitoring

The online monitoring experiment has two configuration parameters (LOAD and TEST_TIME). LOAD represents the input events per second that are input to the two implementations, and TEST_TIME represents the duration of the experiment. They can be configured using `./run_online_monitoring.sh -l <LOAD> -t <TEST_TIME>`. The defaults are `LOAD=5000` and `TEST_TIME=600`. The configuration for the paper is `LOAD=30000` and `TEST_TIME=7200`, but it was run on a server with many powerful cores, so on a laptop `LOAD` should be much lower for the system to be able to handle it. Since the measurements are stable, there is no need to run the experiment for 2 hours (7200 seconds), but if you do want to, feel free to do that.

Run the online monitoring experiment by running `./run_online_monitoring.sh` in the top-level directory. The script prints out output about where to look for the stdout while running the experiment and the results and plots. (Don’t be alarmed if you see an exception at the end of the experiment in the stderr log. That happens because shutting down is abrupt, but doesn’t not affect the measurements or the experiment) For reference, the results and plots are saved in the `data/online-monitoring/server_load_X_time_Y/` directory, where X is the LOAD parameter and Y the TEST_TIME parameter.

The experiment compares the same Flink implementation of the Yahoo Streaming Benchmark (one is sequential and the other parallel). If you want to change the parallelism of the sequential implementation to be parallel, you can do that in line 132 of file `streaming-benchmarks/flink-benchmarks/src/main/java/flink/benchmark/AdvertisingTopologyNative.java` by changing the `computation` second argument from `true` to `false` and then run `./install_online_monitoring.sh`, which reinstalls the experiment.

The results contain three plots (the first two are included in the paper):

- `unmatched_histogram.pdf` which shows a histogram of the unmatched items

- `used_memory_in_time.pdf` which shows the used memory by the matcher as a function of time

- `unmatched_in_time.pdf` which shows the unmatched items as a function of time

The two plots that have time on the x-axis (used_memory_in_time, unmatched_in_time) take samples of used memory and unmatched items every seconds and report that. The unmatched samples are then collected in a histogram in `unmatched_histogram.pdf`.

## Additional Info

### Availability

DiffStream is open source. It can be found on GitHub at `https://github.com/fniksic/diffstream`.

### Source code

The source code is in `src/main`, with tests in `src/test`.
The core algorithm is implemented in `src/main/java/edu/upenn/diffstream/StreamEquivalenceMatcher`.

### Documentation

One challenge in using DiffStream is learning how to write programs using Apache Flink. The [Flink documentation](https://ci.apache.org/projects/flink/flink-docs-stable/) is very helpful, as is Ververica's [Flink Training Exercises](https://github.com/ververica/flink-training-exercises) repository.

Beyond this, while we have not provided separate documentation for the matcher and source code, we point the reader to the "Tutorial" which should be a complete description of how it is used. The examples in this repository can also all be modified, and run with `mvn test` in the appropriate directory.
