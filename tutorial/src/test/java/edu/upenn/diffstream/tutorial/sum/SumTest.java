package edu.upenn.diffstream.tutorial.sum;

import edu.upenn.diffstream.StreamEquivalenceMatcher;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

public class SumTest {

    private static final int PARALLELISM = 3;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testSumParallel() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(PARALLELISM);

        DataStream<DataItem> inputStream = env.fromElements(DataItem.class,
                new Value(1), new Value(7), new Value(-2), new Value(5), new Value(-10), new Barrier(),
                new Value(3), new Value(-1), new Value(2), new Value(4), new Value(-6), new Barrier());

        DataStream<Integer> outputStream = new SumParallel(PARALLELISM).apply(inputStream);
        DataStream<Integer> expectedStream = env.fromElements(1, 3);

        StreamEquivalenceMatcher<Integer> matcher =
                StreamEquivalenceMatcher.createMatcher(outputStream, expectedStream, (x, y) -> true);

        env.execute();

        matcher.assertStreamsAreEquivalent();
    }

    @Test
    public void testSumSequential() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<DataItem> inputStream = env.fromElements(DataItem.class,
                new Value(1), new Value(7), new Value(-2), new Value(5), new Value(-10), new Barrier(),
                new Value(3), new Value(-1), new Value(2), new Value(4), new Value(-6), new Barrier());

        DataStream<Integer> outputStream = new SumSequential().apply(inputStream);
        DataStream<Integer> expectedStream = env.fromElements(1, 3);

        StreamEquivalenceMatcher<Integer> matcher =
                StreamEquivalenceMatcher.createMatcher(outputStream, expectedStream, (x, y) -> true);

        env.execute();

        matcher.assertStreamsAreEquivalent();
    }

    @Test
    public void testDifferential() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(PARALLELISM);

        DataStream<DataItem> inputStream = env.fromElements(DataItem.class,
                new Value(1), new Value(7), new Value(-2), new Value(5), new Value(-10), new Barrier(),
                new Value(3), new Value(-1), new Value(2), new Value(4), new Value(-6), new Barrier());

        DataStream<Integer> sequentialOutput = new SumSequential().apply(inputStream);
        DataStream<Integer> parallelOutput = new SumParallel(PARALLELISM).apply(inputStream);

        StreamEquivalenceMatcher<Integer> matcher =
                StreamEquivalenceMatcher.createMatcher(sequentialOutput, parallelOutput, (x, y) -> true);

        env.execute();

        matcher.assertStreamsAreEquivalent();
    }

    @Test(expected = Exception.class)
    public void testDifferentialSeq() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(PARALLELISM);

        DataStream<DataItem> inputStream = env.fromElements(DataItem.class,
                new Value(1), new Value(7), new Value(-2), new Value(5), new Value(-10), new Barrier(),
                new Value(3), new Value(-1), new Value(2), new Value(4), new Value(-6), new Barrier());

        DataStream<Integer> sequentialOutput = new SumSequential().apply(inputStream);
        DataStream<Integer> parallelOutput = new SumSequential(PARALLELISM).apply(inputStream);

        StreamEquivalenceMatcher<Integer> matcher =
                StreamEquivalenceMatcher.createMatcher(sequentialOutput, parallelOutput, (x, y) -> true);

        env.execute();

        matcher.assertStreamsAreEquivalent();
    }
}
