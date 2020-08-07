package edu.upenn.diffstream.examples.topiccount;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TopicCountParallelExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(TopicCountParallelExperiment.class);

    public static void main(String[] args) throws Exception {
        TopicCountConfig conf = TopicCountConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(conf.getParallelism());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TopicCountItem> inStream =
                env.addSource(new TopicCountSource(conf.getWordsPerDocument(), conf.getTotalDocuments()));
        new TopicCountParallel(conf.getParallelism()).apply(inStream);

        JobExecutionResult result = env.execute();

        LOG.info("Total time: {} ms", result.getNetRuntime(TimeUnit.MILLISECONDS));
    }
}
