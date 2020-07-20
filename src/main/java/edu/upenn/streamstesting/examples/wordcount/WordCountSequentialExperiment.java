package edu.upenn.streamstesting.examples.wordcount;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class WordCountSequentialExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountSequentialExperiment.class);

    public static void main(String[] args) throws Exception {
        WordCountConfig conf = WordCountConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<WordCountItem> inStream =
                env.addSource(new WordCountSource(conf.getWordsPerDocument(), conf.getTotalDocuments()));
        new WordCountSequential().apply(inStream);

        JobExecutionResult result = env.execute();

        LOG.info("Total time: {} ms", result.getNetRuntime(TimeUnit.MILLISECONDS));
    }
}
