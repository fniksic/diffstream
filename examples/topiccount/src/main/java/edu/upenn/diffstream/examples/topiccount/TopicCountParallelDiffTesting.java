package edu.upenn.diffstream.examples.topiccount;

import edu.upenn.diffstream.FullDependence;
import edu.upenn.diffstream.StreamsNotEquivalentException;
import edu.upenn.diffstream.remote.RemoteMatcherFactory;
import edu.upenn.diffstream.remote.RemoteStreamEquivalenceMatcher;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class TopicCountParallelDiffTesting {

    public static void main(String[] args) throws Exception {
        TopicCountConfig conf = TopicCountConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<TopicCountItem> inStream =
                env.addSource(new TopicCountSource(conf.getWordsPerDocument(), conf.getTotalDocuments()));
        DataStream<String> outStreamExpected = new TopicCountSequential().apply(inStream);
        DataStream<String> outStreamParallel = new TopicCountParallel(conf.getParallelism()).apply(inStream);

        RemoteMatcherFactory.init();
        RemoteStreamEquivalenceMatcher<String> matcher =
                RemoteMatcherFactory.getInstance().createMatcher(outStreamExpected, outStreamParallel, new FullDependence<>());

        try {
            JobExecutionResult result = env.execute();
            matcher.assertStreamsAreEquivalent();
            System.out.println("Streams are equivalent!");
            System.out.println("Total time: " + result.getNetRuntime(TimeUnit.MILLISECONDS) + " ms");
        } catch (Exception e) {
            if (StreamsNotEquivalentException.isRootCauseOf(e)) {
                System.out.println("Streams are NOT equivalent!");
            } else {
                throw e;
            }
        } finally {
            RemoteMatcherFactory.destroy();
        }
    }

}
