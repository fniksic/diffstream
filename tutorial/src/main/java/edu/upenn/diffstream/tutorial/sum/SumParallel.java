package edu.upenn.diffstream.tutorial.sum;

import edu.upenn.diffstream.FlinkProgram;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * A Flink computation that maintains a cumulative sum of the values ({@link Value})
 * in the data stream, and outputs the sum whenever it encounters a barrier ({@link Barrier}).
 *
 * Unlike {@link SumSequential}, this version of the computation is truly parallel.
 * The values between two barriers will be summed up using a specified number of parallel
 * operator instances.
 */
public class SumParallel implements FlinkProgram<DataItem, Integer>, Serializable {

    private static final long serialVersionUID = 8928500548491154569L;

    private int parallelism;

    public SumParallel() {
        this(1);
    }

    public SumParallel(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public DataStream<Integer> apply(DataStream<DataItem> inputStream) {
        // We start by assigning watermarks in place of Barriers
        DataStream<DataItem> timedStream = inputStream.assignTimestampsAndWatermarks(new BarrierToWatermarkAssigner());

        // We wrap the Values into ValuesWithId, where the id is based on the
        // position in the input stream. We drop Barriers since now we have watermarks instead.
        // It is important to set parallelism to 1 for this operator, since it is stateful.
        DataStream<ValueWithId> wrappedStream = timedStream.flatMap(new DataItemToValueWithId()).setParallelism(1);

        // Next, we partition the stream into buckets based on the introduced id, which allows
        // us to split into 1 millisecond time windows, aggregate each partition in parallel,
        // and reduce the partitions to obtain the partial sums for each 1 millisecond window.
        DataStream<Integer> partialSums = wrappedStream
                .keyBy(x -> x.getId() % parallelism)
                .timeWindow(Time.milliseconds(1L))
                .aggregate(new ValueWithIdAggregator()).setParallelism(parallelism)
                .timeWindowAll(Time.milliseconds(1L))
                .reduce(Integer::sum);

        // Finally, we obtain the cumulative sums. In Flink, we must assign the elements
        // into a single global window. By default, this window is never finished, so any
        // aggregate function never triggers. That is why we explicitly assign a trigger that
        // triggers after every new element before finally calling reduce.
        DataStream<Integer> cumulativeSums = partialSums
                .windowAll(GlobalWindows.create())
                .trigger(CountTrigger.of(1))
                .reduce(Integer::sum);

        return cumulativeSums;
    }

    private static class BarrierToWatermarkAssigner implements AssignerWithPunctuatedWatermarks<DataItem> {
        private long barrierCount = 0;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(DataItem dataItem, long extractedTimestamp) {
            if (dataItem instanceof Barrier) {
                barrierCount++;
                return new Watermark(extractedTimestamp);
            } else {
                return null;
            }
        }

        @Override
        public long extractTimestamp(DataItem dataItem, long previousElementTimestamp) {
            return barrierCount;
        }
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

    private static class DataItemToValueWithId implements FlatMapFunction<DataItem, ValueWithId> {
        private long count = 0L;

        @Override
        public void flatMap(DataItem dataItem, Collector<ValueWithId> collector) {
            if (dataItem instanceof Value) {
                int val = ((Value) dataItem).getVal();
                collector.collect(new ValueWithId(val, count++));
            }
        }
    }

    private static class ValueWithIdAggregator implements AggregateFunction<ValueWithId, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(ValueWithId valueWith, Integer acc) {
            return acc + valueWith.getVal();
        }

        @Override
        public Integer getResult(Integer acc) {
            return acc;
        }

        @Override
        public Integer merge(Integer acc1, Integer acc2) {
            return acc1 + acc2;
        }
    }
}
