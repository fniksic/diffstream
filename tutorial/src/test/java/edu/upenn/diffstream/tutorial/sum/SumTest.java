package edu.upenn.diffstream.tutorial.sum;

import edu.upenn.diffstream.matcher.MatcherFactory;
import edu.upenn.diffstream.matcher.StreamEquivalenceMatcher;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * A test class with several test regarding the {@link SumSequential} and {@link SumParallel}
 * Flink programs, demonstrating how {@link StreamEquivalenceMatcher} can be used for differential
 * testing of these programs.
 */
public class SumTest {

    private static final int PARALLELISM = 4;

    /**
     * Flink provides a mini cluster that is suitable for testing purposes. We
     * configure the cluster to have 1 task manager with {@code PARALLELISM + 1} task slots,
     * which allows a total parallelism of {@code PARALLELISM + 1}.
     */
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(PARALLELISM + 1)
                            .setNumberTaskManagers(1)
                            .build());

    /**
     * The first test runs {@link SumParallel} on a fixed input and checks that we get the expected
     * result.
     *
     * @throws Exception
     */
    @Test
    public void testSumParallel() throws Exception {

        // We get an execution environment on a mini cluster, set the default parallelism to 1
        // (parallelism is explicitly set to {@code PARALLELISM} below for the operators we
        // wish to parallelize), and set the time characteristic to event time. The last bit
        // is needed because {@link SumParallel} relies on event time processing to implement
        // parallel execution.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // We use a fixed input stream.
        DataStream<DataItem> inputStream = env.fromElements(DataItem.class,
                new Value(1), new Value(7), new Value(-2), new Value(5), new Value(-10), new Barrier(),
                new Value(3), new Value(-1), new Value(2), new Value(4), new Value(-6), new Barrier());

        // We apply {@link SumParallel} on the input stream and compare the output to manually
        // provided expected result.
        DataStream<Integer> outputStream = new SumParallel(PARALLELISM).apply(inputStream);
        DataStream<Integer> expectedStream = env.fromElements(1, 3);

        // In the matcher we use a full dependence relation (everything is dependent), given as a
        // lambda expression {@code (x, y) -> true} that always returns {@code true}. This means we
        // want to compare {@code outputStream} and {@code expectedStream} as sequences.
        try (var matcher =
                MatcherFactory.createMatcher(outputStream, expectedStream, (x, y) -> true)) {

            // We execute the program on the environment and assert that the streams are equivalent.
            env.execute();
            matcher.assertStreamsAreEquivalent();
        }
    }

    /**
     * The second test runs {@link SumSequential} on a fixed input and checks that we get the expected
     * result.
     *
     * @throws Exception
     */
    @Test
    public void testSumSequential() throws Exception {
        // As usual, we get an execution environment and set the default parallelism to 1.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<DataItem> inputStream = env.fromElements(DataItem.class,
                new Value(1), new Value(7), new Value(-2), new Value(5), new Value(-10), new Barrier(),
                new Value(3), new Value(-1), new Value(2), new Value(4), new Value(-6), new Barrier());

        // We apply {@link SumSequential} on a fixed input stream and compare the result to manually
        // provided expected result.
        DataStream<Integer> outputStream = new SumSequential().apply(inputStream);
        DataStream<Integer> expectedStream = env.fromElements(1, 3);

        // As above, we use a full dependence relation to compare the output streams as sequences.
        try (var matcher =
                MatcherFactory.createMatcher(outputStream, expectedStream, (x, y) -> true)) {
            env.execute();
            matcher.assertStreamsAreEquivalent();
        }
    }

    /**
     * In this test we run a differential test between {@link SumSequential} and {@link SumParallel},
     * comparing their results in parallel in an online manner.
     *
     * @throws Exception
     */
    @Test
    public void testDifferential() throws Exception {
        // As before, we get the execution environment, set the default parallelism to 1 and the
        // time characteristic to event time.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // We run the test on fixed input.
        DataStream<DataItem> inputStream = env.fromElements(DataItem.class,
                new Value(1), new Value(7), new Value(-2), new Value(5), new Value(-10), new Barrier(),
                new Value(3), new Value(-1), new Value(2), new Value(4), new Value(-6), new Barrier());

        // We apply both programs on the same input.
        DataStream<Integer> sequentialOutput = new SumSequential().apply(inputStream);
        DataStream<Integer> parallelOutput = new SumParallel(PARALLELISM).apply(inputStream);

        // We run the matcher with full dependence relation, comparing the outputs as sequences.
        try (var matcher =
                MatcherFactory.createMatcher(sequentialOutput, parallelOutput, (x, y) -> true)) {
            env.execute();
            matcher.assertStreamsAreEquivalent();
        }
    }

    /**
     * In the final test we run a differential test between two versions of {@link SumSequential}, one truly
     * sequential and the other with increased parallelism. The test demonstrates that it is not correct to
     * simply set the parallelism of {@link SumSequential} to more than 1. We state that the matcher is
     * expected to throw an exception in the @Test annotation.
     *
     * @throws Exception
     */
    @Test(expected = Exception.class)
    public void testDifferentialSeq() throws Exception {
        // As before, we get the execution environment and set the default parallelism to 1.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // We run the test on fixed input.
        DataStream<DataItem> inputStream = env.fromElements(DataItem.class,
                new Value(1), new Value(7), new Value(-2), new Value(5), new Value(-10), new Barrier(),
                new Value(3), new Value(-1), new Value(2), new Value(4), new Value(-6), new Barrier());

        // We apply both programs on the same input.
        DataStream<Integer> sequentialOutput = new SumSequential().apply(inputStream);
        DataStream<Integer> parallelOutput = new SumSequential(PARALLELISM).apply(inputStream);

        // We run the matcher with full dependence relation, comparing the outputs as sequences.
        try (var matcher =
                MatcherFactory.createMatcher(sequentialOutput, parallelOutput, (x, y) -> true)) {
            env.execute();
            matcher.assertStreamsAreEquivalent();
        }
    }

}
