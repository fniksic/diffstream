package edu.upenn.streamstesting.examples.punctuation;

import edu.upenn.streamstesting.FlinkProgram;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class SumValuesSequentially implements FlinkProgram<DataItem, Integer>, Serializable {

    private static final long serialVersionUID = 8796869879965796517L;

    @Override
    public DataStream<Integer> apply(DataStream<DataItem> inputStream) {
        return inputStream.flatMap(new FlatMapFunction<DataItem, Integer>() {
            private int sum = 0;

            @Override
            public void flatMap(DataItem dataItem, Collector<Integer> collector) throws Exception {
                if (dataItem instanceof Value) {
                    sum += ((Value) dataItem).getVal();
                } else {
                    collector.collect(sum);
                }
            }
        }).setParallelism(1);
    }
}
