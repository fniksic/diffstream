package edu.upenn.streamstesting.examples.wordcount;

import edu.upenn.streamstesting.EmptyDependence;
import edu.upenn.streamstesting.FullDependence;
import edu.upenn.streamstesting.StreamEquivalenceMatcher;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountTest {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountTest.class);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(2)
                            .build());

    @Ignore
    @Test(expected = Exception.class)
    public void testWordCountSequential() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStream<WordCountItem> inStream = env.fromElements(WordCountItem.class,
                new Word("a"), new Word("math"), new Word("larry"), new Word("boat"), new EndOfFile(),
                new Word("math"), new Word("a"), new Word("boat"), new Word("larry"), new EndOfFile());
        DataStream<String> outStreamSeq = new WordCountSequential().apply(inStream);
        DataStream<String> outStreamPar = new WordCountSequential(4).apply(inStream);
        StreamEquivalenceMatcher<String> matcher =
                StreamEquivalenceMatcher.createMatcher(outStreamSeq, outStreamPar, new FullDependence<>());
        env.execute();
        matcher.assertStreamsAreEquivalent();
    }

    @Ignore
    @Test
    public void testWordCountParallel() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        DataStream<WordCountItem> inStream = env.fromElements(WordCountItem.class,
                new Word("a"), new Word("math"), new Word("larry"), new Word("boat"), new EndOfFile(),
                new Word("math"), new Word("a"), new Word("boat"), new Word("larry"), new EndOfFile());
        DataStream<String> outStreamSeq = new WordCountSequential().apply(inStream);
        DataStream<String> outStreamPar = new WordCountParallel(4).apply(inStream);
        StreamEquivalenceMatcher<String> matcher =
                StreamEquivalenceMatcher.createMatcher(outStreamSeq, outStreamPar, new EmptyDependence<>());
        env.execute();
        matcher.assertStreamsAreEquivalent();
    }

    @Test
    public void testWordCountSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        int wordsPerDocument = 10;
        int totalDocuments = 10;
        DataStream<Integer> out = env.addSource(new WordCountSource(wordsPerDocument, totalDocuments))
                .flatMap(new FlatMapFunction<WordCountItem, Integer>() {
                             @Override
                             public void flatMap(WordCountItem item, Collector<Integer> collector) throws Exception {
                                 item.<Void>match(
                                         word -> {
                                             collector.collect(1);
                                             return null;
                                         },
                                         endOfFile -> null
                                 );
                             }
                         })
                .countWindowAll(wordsPerDocument * totalDocuments)
                .sum(0);
        DataStream<Integer> expected = env.fromElements(wordsPerDocument * totalDocuments);
        StreamEquivalenceMatcher<Integer> matcher =
                StreamEquivalenceMatcher.createMatcher(out, expected, new FullDependence<>());
        env.execute();
        matcher.assertStreamsAreEquivalent();
    }
}
