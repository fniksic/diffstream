# DiffStream

A differential testing library for [Apache Flink](https://flink.apache.org/) programs.

The tool and the underlying methodology are described in the OOPSLA paper, which can be found in `PAPER.pdf`.
This README contains: (1) instructions to quickly get started, (2) a detailed tutorial on how to use the tool, (3) how to re-run the experiments from the paper and interpret the results, and (4) additional documentation for reference.

## Getting Started Guide

**Virtual Machine:** The virtual machine should come with all dependencies installed.
(For later users, the installation instructions can be found in `INSTALL.md`.) Here is the required info to log in:

```
Username: diffstream
Password: diffstream
```

Note: We suggest running the VM with at least 6GB of RAM and 2 processor cores so that everything runs smoothly. With less than 2 cores, there won't be visible scalability in the Topic Count experiment.

**Addendum: running the VM**
[VirtualBox](https://www.virtualbox.org/) can be used to open the VM.
Once the machine loads, you can then find DiffStream in Files (~/diffstream).
Open `README.md`, which should be identical to this file, *except* for missing this paragraph.
Please note that many tests in this file will print out "WARNING:" statements unrelated to DiffStream, but related to certain dependencies; these can safely be ignored.
Finally, for the Tutorial and Step-by-Step Guide below, we recommend using IntelliJ to open and browse source code .java files. It should be installed (search for it in the dash).

DiffStream is a testing tool. The tool is used by writing two Flink programs (using the Java API), providing a specification of correct ordering behavior, and then connecting the output to the DiffStream matcher. DiffStream either succeeds (normal termination) or reports a bug (raises StreamsNotEquivalentException). For more details on how to use it yourself or modify the existing examples, see the (optional) "Tutorial" below.

To check that the tool is working properly, you can run `mvn test` (in the top-level directory, where this README is). This runs unit tests.

To quickly validate the paper experiments, we provide the following 4 shell scripts:

- **(Section 5.1 case study, ~10 seconds)** Run `./run_taxi.sh`. Verify that 5 tests are run and that the build succeeds.

- **(Section 5.2 case study, ~10 minutes)**
Run `./run_topiccount.sh` and check that it doesn't fail. It should print a sequence of jobs. The first few are DiffStream tests of a program, and a total time to complete each test, and the rest are running times of the parallel program to verify that it speeds up with increasing levels of parallelism.

- **(Section 5.3 case study, ~1 minute)** Run `./run_mapreduce.sh`. Verify that the 12 test results pass and the script says BUILD SUCCEEDED. (Each checkmark in the Section 5.3 table corresponds to one test result, except the final row, StrConcat, where the two checkmarks correspond to 4 tests.) Please note that because these are random tests, it is possible (though unlikely) that a test may not detect the bug, and the script will fail. If so, run the script again.

- **(Section 5.4 case study, ~5 minutes)** Run `./run_online_monitoring.sh -l 1000 -t 120` and after it finishes, check that 3 plots have been produced in `data/online-monitoring//server_load_1000_time_120_leftpar_1_right_par_2`.

More detail on each of these case studies can be found under "Running the Experiments".

## Tutorial

To get the reader familiar with DiffStream and Flink, we have decided to include a short tutorial. The tutorial consists of reading and optionally altering a few simple DiffStream tests, and it should give the reader an idea of how to test Flink programs. DiffStream can be used to check equivalence of streams (up to reordering), to check the output of a Flink program against a manually provided output, and to run a differential test between two programs.

The relevant source code for the tutorial is located in the directory `diffstream/tutorial`. In the remainder of this section, all the paths will be given relative to this directory. As a sanity check, running `mvn test` in the directory should complete successfully, with a total of 6 successfully executed tests.

### IncDec

We start with the IncDec example, whose source files are located in `src/main/java/edu/upenn/diffstream/tutorial/incdec`. There are 5 files there, defining an interface called `IncDecItem` with three implementing classes called `Inc`, `Dec`, and `Barrier`, and a dependence relation implemented as a class called `IncDecDependence`. `IncDecItem` is the base type of events in a stream: `Inc` and `Dec` represent increment and decrement events and they hold a non-negative integer value, and `Barrier` represents events that delineate sets of increments and decrements in a stream. The dependence relation among these event types is defined so that two increments are independent, two decrements are independent, and every other combination of two events is dependent. The comments in the source files should provide more intuition behind these event types and the dependence relation.

We are now ready to take a look at the tests. The file `src/test/java/edu/upenn/diffstream/tutorial/incdec/IncDecTest.java` contains two JUnit tests. Each test starts by defining a Flink stream execution environment, in which we manually define two streams. We connect the streams in a stream equivalence matcher, together with an instance of the `IncDecDependence` dependence relation. By doing this, we have defined a Flink program that will feed the two streams into the matcher, which will then check whether the streams are equivalent up to reordering with respect to the dependence relation. We execute the program by running `env.execute()`. The program is executed on an embedded mini cluster provided by Flink for testing, which is defined and configured at the top of the IncDecTest class.

In the one of the two tests (`IncDecTest.testIncDecNonEquivalent`), the two streams are not equivalent, that is, the first stream cannot be reordered to get the second. The matcher will detect this and throw an exception, but the test is annotated to expect an exception, so it will pass. The other test (`IncDecTest.testIncDecEquivalent`) has two equivalent streams.

In order to run the tests from the console, execute

```
mvn test -Dtest=IncDecTest
```

It is also possible to run a specific test:

```
mvn test -Dtest=IncDecTest#testIncDecNonEquivalent
mvn test -Dtest=IncDecTest#testIncDecEquivalent
```

The tests can also be run in IntelliJ IDEA, which is provided in the virtual machine.

It may be interesting to see what happens when we change the dependence relation. Try to edit the code in `IncDecDependence.java` and replace the body of the `test` method to simply always return `false`. Before re-running the tests, try to predict which one (if any) will fail, then re-run the tests. Now change the `IncDecDependence.test` method to always return `true`, and repeat the same experiment.

### Sum

In the next part of the tutorial, we will look at the Sum example, whose source code is located in `src/main/java/edu/upenn/diffstream/tutorial/sum`. Here we have two event types, `Value` and `Barrier`, both of which implement a common interface called `DataItem`. `Value` carries an integer value. In this example, there are two versions of a Flink program that computes a cumulative sum of the values in a stream, and outputs the sum each time it encounters a `Barrier`. The simpler version is sequential (`SumSequential.java`), and the more complex version is parallel (`SumParallel.java`). Try to get a basic understanding of how the sequential version works by following the comments in the source code. Understanding the parallel version is challenging and it requires more advanced understanding of Flink. For this tutorial it suffices to notice that it is not immediately clear the two versions of the program are equivalent.

We try to establish the equivalence of the programs by testing whether they produce the same output on the same input. There are four tests located in `src/test/java/edu/upenn/diffstream/tutorial/sum/SumTest.java`:

```
mvn test -Dtest=SumTest#testSumParallel
mvn test -Dtest=SumTest#testSumSequential
mvn test -Dtest=SumTest#testDifferential
mvn test -Dtest=SumTest#testDifferentialSeq
```

The general outline of the tests is the same as in the previous example. The difference is that instead of testing whether fixed streams are equivalent, we are testing whether the outputs of programs are equivalent. The first two tests compare the output of the sequential and the parallel program with a manually provided stream, resembling a traditional unit test. The last two tests are differential tests. `SumTest.testDifferential` compares the parallel program against the sequential program, with the latter essentially serving as a form of specification for what the former should output. `SumTest.testDifferentialSeq` compares a parallelized version of the sequential program against the sequential program itself. Here, the parallelization is achieved by simply instantiating several instances of the program, which is shown not to be correct by the test.

We suggest tweaking the constant called `PARALLELISM` defined on top of `SumTest`. Try setting it to 1. Will all the tests succeed? Another interesting thing to try is to change the input stream in `SumTest.testDifferentialSeq`. Even if the stream contains only one `Value` and one `Barrier`, and provided that `PARALLELISM` is more than 1, the test should reveal the two programs are not equivalent. We will encounter the same phenomenon again in the Topic Count case study, which is essentially a more realistic version of the Sum example.

## Step By Step Instructions: Running the Experiments

These instructions are divided based on the section of the paper that they correspond to.

### 5.1 Taxi Distance

The source code of the Taxi Distance experiment is located in
`examples/taxi`. To run all the TaxiDistance tests, you can just use
the script `./run_taxi.sh`, but alternatively you can use IntelliJ to open the
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

To only verify the results, the top-level script `run_topiccount.sh` should produce output that looks something like the following:

```
Starting the sequential Topic Count
...
Total time: 53035 ms
Starting the parallel Topic Count
...
Total time: 26457 ms
Starting the sequential differential test

Job has been submitted with JobID 95fc222c67ce78f2672a1c42c081ff12
Streams are NOT equivalent!
Starting the parallel differential test

Job has been submitted with JobID 693c09bc5c605f000bf235daec6702ba
Program execution finished
Job with JobID 693c09bc5c605f000bf235daec6702ba has finished.
Job Runtime: 75745 ms

Streams are equivalent!
Total time: 75745 ms
Starting the parallel Topic Count with parallelism 1
...
Total time: 60813 ms
---

Starting the parallel Topic Count with parallelism 2
...
Total time: 43663 ms
---

Starting the parallel Topic Count with parallelism 3
...
Total time: 33977 ms
---

Starting the parallel Topic Count with parallelism 4
...
Total time: 29335 ms
---

Starting the parallel Topic Count with parallelism 5
...
Total time: 29460 ms
---

Starting the parallel Topic Count with parallelism 6
...
Total time: 25763 ms
---

Starting the parallel Topic Count with parallelism 7
...
Total time: 24890 ms
---

Starting the parallel Topic Count with parallelism 8
...
Total time: 24973 ms
---

Summary of the results:

parallelism	time (ms)	speedup
1		60813		1.00
2		43663		1.39
3		33977		1.78
4		29335		2.07
5		29460		2.06
6		25763		2.36
7		24890		2.44
8		24973		2.43
...
```

where `...` mean possibly several lines.

We now go through the experiments one by one in more detail. From the paper, the goal in this case study is to implement a streaming program for finding the most frequent word topic in a document. Documents are given as a stream of words delineated with end-of-file markers, and the mapping from words to topics is stored in an external Redis database. As explained in the paper, there is a fairly straightforward sequential solution and a tricky-to-implement parallel solution to this problem. The two solutions are located in files `src/main/java/edu/upenn/diffstream/examples/topiccount/TopicCountSequential.java` and `src/main/java/edu/upenn/diffstream/examples/topiccount/TopicCountParallel.java`. The first step of the case study is to look at the code and convince yourself that the parallel version looks dauntingly tricky and quite different then the sequential version.

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

## Additional Information

### Summary of claims in the paper supported by the artifact VM

This artifact supports the claims in the paper made in Section 5.

The case studies in sections 5.1 and 5.3 are fully supported by the artifact, as all of the tests can be re-run and verified.

The case studies in 5.2 and 5.4 are partially supported. All of the tests can be re-run, but the performance numbers differ due to being run in a VM. In 5.2, the parallelism shows scalability of 2.5, not as parallel as in the paper. In 5.4, the trends for memory usage appear similar to what is in the paper.

### Availability

DiffStream is open source. It can be found on GitHub at `https://github.com/fniksic/diffstream`.

### Source code

The source code is in `src/main`, with tests in `src/test`.
The core algorithm is implemented in `src/main/java/edu/upenn/diffstream/StreamEquivalenceMatcher`.

### Additional Documentation

One challenge in using DiffStream is learning how to write programs using Apache Flink. The [Flink documentation](https://ci.apache.org/projects/flink/flink-docs-stable/) is very helpful, as is Ververica's [Flink Training Exercises](https://github.com/ververica/flink-training-exercises) repository.

While we have not provided separate documentation for the matcher and source code, this file (including the Tutorial) should be complete description of how it is used. The examples in this repository can also all be modified, and run with `mvn test` in the appropriate directory.
