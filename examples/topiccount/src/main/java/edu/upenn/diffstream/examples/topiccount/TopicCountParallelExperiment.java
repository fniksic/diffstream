package edu.upenn.diffstream.examples.topiccount;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class TopicCountParallelExperiment {

    public static void main(String[] args) throws Exception {
        TopicCountConfig conf = TopicCountConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TopicCountItem> inStream =
                env.addSource(new TopicCountSource(conf.getWordsPerDocument(), conf.getTotalDocuments()));
        DataStream<String> outStream = new TopicCountParallel(conf.getParallelism()).apply(inStream);

        outStream.writeAsText("topics.txt", FileSystem.WriteMode.OVERWRITE);

        JobExecutionResult result = env.execute();

        System.out.println("Total time: " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " ms");
    }

}
