package edu.upenn.diffstream.examples.topiccount;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TopicCountSequentialExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(TopicCountSequentialExperiment.class);

    public static void main(String[] args) throws Exception {
        TopicCountConfig conf = TopicCountConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<TopicCountItem> inStream =
                env.addSource(new TopicCountSource(conf.getWordsPerDocument(), conf.getTotalDocuments()));
        new TopicCountSequential().apply(inStream);

        JobExecutionResult result = env.execute();

        LOG.info("Total time: {} ms", result.getNetRuntime(TimeUnit.MILLISECONDS));
    }
}
