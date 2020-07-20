package edu.upenn.streamstesting.examples.wordcount;

import edu.upenn.streamstesting.FullDependence;
import edu.upenn.streamstesting.StreamsNotEquivalentException;
import edu.upenn.streamstesting.remote.RemoteMatcherFactory;
import edu.upenn.streamstesting.remote.RemoteStreamEquivalenceMatcher;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class WordCountSequentialDiffTesting {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountSequentialDiffTesting.class);

    public static void main(String[] args) throws Exception {
        WordCountConfig conf = WordCountConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1 + conf.getParallelism());

        DataStream<WordCountItem> inStream =
                env.addSource(new WordCountSource(conf.getWordsPerDocument(), conf.getTotalDocuments()));
        DataStream<String> outStreamExpected = new WordCountSequential().apply(inStream);
        DataStream<String> outStreamParallel = new WordCountSequential(conf.getParallelism()).apply(inStream);

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
