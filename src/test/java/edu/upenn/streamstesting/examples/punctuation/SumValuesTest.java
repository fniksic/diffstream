package edu.upenn.streamstesting.examples.punctuation;

import edu.upenn.streamstesting.StreamEquivalenceMatcher;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

public class SumValuesTest {

    private static final int PARALLELISM = 3;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testSumValuesInParallel() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(PARALLELISM);

        DataStream<DataItem> inputStream = env.fromElements(DataItem.class,
                new Value(1), new Value(7), new Value(-2), new Value(5), new Value(-10), new Barrier(),
                new Value(3), new Value(-1), new Value(2), new Value(4), new Value(-6), new Barrier());

        DataStream<Integer> outputStream = new SumValuesInParallel(PARALLELISM).apply(inputStream);
        DataStream<Integer> expectedStream = env.fromElements(1, 3);

        StreamEquivalenceMatcher<Integer> matcher =
                StreamEquivalenceMatcher.createMatcher(outputStream, expectedStream, (x, y) -> true);

        env.execute();

        matcher.assertStreamsAreEquivalent();
    }

    @Test
    public void testSumValuesSequentially() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<DataItem> inputStream = env.fromElements(DataItem.class,
                new Value(1), new Value(7), new Value(-2), new Value(5), new Value(-10), new Barrier(),
                new Value(3), new Value(-1), new Value(2), new Value(4), new Value(-6), new Barrier());

        DataStream<Integer> outputStream = new SumValuesSequentially().apply(inputStream);
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

        DataStream<Integer> sequentialOutput = new SumValuesSequentially().apply(inputStream);
        DataStream<Integer> parallelOutput = new SumValuesInParallel(PARALLELISM).apply(inputStream);

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

        DataStream<Integer> sequentialOutput = new SumValuesSequentially().apply(inputStream);
        DataStream<Integer> parallelOutput = new SumValuesSequentially(PARALLELISM).apply(inputStream);

        StreamEquivalenceMatcher<Integer> matcher =
                StreamEquivalenceMatcher.createMatcher(sequentialOutput, parallelOutput, (x, y) -> true);

        env.execute();

        matcher.assertStreamsAreEquivalent();
    }

    @Test
    public void testExplicitly() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(PARALLELISM);

        DataStream<DataItem> stream = env.fromElements(DataItem.class,
                new Value(1), new Value(7), new Value(-2), new Value(5), new Value(-10), new Barrier(),
                new Value(3), new Value(-1), new Value(2), new Value(4), new Value(-6), new Barrier());

        DataStream<DataItem> timedStream =
                stream.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<DataItem>() {
                    private long barrierCount = 0;

                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(DataItem dataItem, long extractedTimestamp) {
                        if (dataItem instanceof Barrier) {
                            barrierCount++;
                            return new Watermark(extractedTimestamp);
                        }
                        return null;
                    }

                    @Override
                    public long extractTimestamp(DataItem dataItem, long previousElementTimestamp) {
                        return barrierCount;
                    }
                });

        DataStream<ValueWithId> wrappedStream = timedStream
                .flatMap(new FlatMapFunction<DataItem, ValueWithId>() {
                    private long count = 0L;

                    @Override
                    public void flatMap(DataItem dataItem, Collector<ValueWithId> collector) throws Exception {
                        if (dataItem instanceof Value) {
                            int val = ((Value) dataItem).getVal();
                            collector.collect(new ValueWithId(val, count++));
                        }
                    }
                }).setParallelism(1);

        DataStream<Integer> partialSums = wrappedStream.keyBy(x -> x.getId() % PARALLELISM)
                .timeWindow(Time.of(1L, TimeUnit.MILLISECONDS))
                .aggregate(new AggregateFunction<ValueWithId, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(ValueWithId valueWithId, Integer acc) {
                        return acc + valueWithId.getVal();
                    }

                    @Override
                    public Integer getResult(Integer acc) {
                        return acc;
                    }

                    @Override
                    public Integer merge(Integer acc1, Integer acc2) {
                        return acc1 + acc2;
                    }
                })
                .timeWindowAll(Time.of(1L, TimeUnit.MILLISECONDS))
                .reduce((x, y) -> x + y);

        DataStream<Integer> cumulativeSums = partialSums
                .windowAll(GlobalWindows.create())
                .trigger(CountTrigger.of(1))
                .reduce((x, y) -> x + y);

        cumulativeSums.print().setParallelism(1);

        env.execute();
        // We should see 1 followed by 3 as output
    }

    private static class ValueWithId {
        private final int val;
        private final long id;

        public ValueWithId(int val, long id) {
            this.val = val;
            this.id = id;
        }

        public int getVal() {
            return val;
        }

        public long getId() {
            return id;
        }
    }
}
