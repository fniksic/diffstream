package edu.upenn.diffstream.tutorial.sum;

import edu.upenn.diffstream.FlinkProgram;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * A Flink computation that maintains a cumulative sum of the values ({@link Value})
 * in the data stream, and outputs the sum whenever it encounters a barrier ({@link Barrier}).
 *
 * The computation is implemented sequentially using a flatMap operator. The operator
 * stores the cumulative sum as a local state, and therefore it doesn't make sense to
 * parallelize it. However, we enable setting the parallelism parameter to more than 1
 * for illustrative purposes.
 */
public class SumSequential implements FlinkProgram<DataItem, Integer>, Serializable {

    private static final long serialVersionUID = 8796869879965796517L;

    private int parallelism;

    public SumSequential() {
        this(1);
    }

    public SumSequential(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public DataStream<Integer> apply(DataStream<DataItem> inputStream) {
        return inputStream.flatMap(new FlatMapFunction<DataItem, Integer>() {

            // We maintain the cumulative sum as a local state of the flatMap
            // operator.
            private int sum = 0;

            @Override
            public void flatMap(DataItem dataItem, Collector<Integer> collector) {
                if (dataItem instanceof Value) {
                    sum += ((Value) dataItem).getVal();
                } else {
                    collector.collect(sum);
                }
            }
        }).setParallelism(parallelism);
    }
}
