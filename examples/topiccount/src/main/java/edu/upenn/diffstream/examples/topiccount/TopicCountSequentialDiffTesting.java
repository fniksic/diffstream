package edu.upenn.diffstream.examples.topiccount;

import edu.upenn.diffstream.FullDependence;
import edu.upenn.diffstream.matcher.MatcherFactory;
import edu.upenn.diffstream.matcher.StreamsNotEquivalentException;
import edu.upenn.diffstream.monitor.ResourceMonitor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class TopicCountSequentialDiffTesting {

    public static void main(String[] args) throws Exception {
        TopicCountConfig conf = TopicCountConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<TopicCountItem> inStream =
                env.addSource(new TopicCountSource(conf.getWordsPerDocument(), conf.getTotalDocuments()));
        DataStream<String> outStreamExpected = new TopicCountSequential().apply(inStream);
        DataStream<String> outStreamParallel = new TopicCountSequential(conf.getParallelism()).apply(inStream);

        MatcherFactory.initRemote();
        try (var matcher =
                     MatcherFactory.createRemoteMatcher(outStreamExpected, outStreamParallel, new FullDependence<>());
             var monitor =
                     new ResourceMonitor(matcher, "unmatched-items.txt", "memory-log.txt")) {
            monitor.start();
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
            MatcherFactory.destroyRemote();
        }
    }

}
