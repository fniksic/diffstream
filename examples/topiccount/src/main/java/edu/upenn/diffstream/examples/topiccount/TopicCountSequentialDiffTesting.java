package edu.upenn.diffstream.examples.topiccount;

import edu.upenn.diffstream.FullDependence;
import edu.upenn.diffstream.StreamsNotEquivalentException;
import edu.upenn.diffstream.remote.RemoteMatcherFactory;
import edu.upenn.diffstream.remote.RemoteStreamEquivalenceMatcher;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TopicCountSequentialDiffTesting {

    private static final Logger LOG = LoggerFactory.getLogger(TopicCountSequentialDiffTesting.class);

    public static void main(String[] args) throws Exception {
        TopicCountConfig conf = TopicCountConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1 + conf.getParallelism());

        DataStream<TopicCountItem> inStream =
                env.addSource(new TopicCountSource(conf.getWordsPerDocument(), conf.getTotalDocuments()));
        DataStream<String> outStreamExpected = new TopicCountSequential().apply(inStream);
        DataStream<String> outStreamParallel = new TopicCountSequential(conf.getParallelism()).apply(inStream);

        RemoteMatcherFactory.init();
        RemoteStreamEquivalenceMatcher<String> matcher =
                RemoteMatcherFactory.getInstance().createMatcher(outStreamExpected, outStreamParallel, new FullDependence<>());

        try {
            JobExecutionResult result = env.execute();
            matcher.assertStreamsAreEquivalent();
            LOG.info("Total time: {} ms", result.getNetRuntime(TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            if (e instanceof StreamsNotEquivalentException ||
                    e.getCause() instanceof StreamsNotEquivalentException ||
                    (e.getCause() != null && e.getCause().getCause() instanceof StreamsNotEquivalentException)) {
                LOG.info("Streams are NOT equivalent!");
            } else {
                throw e;
            }
        }

        RemoteMatcherFactory.destroy();
    }
}
