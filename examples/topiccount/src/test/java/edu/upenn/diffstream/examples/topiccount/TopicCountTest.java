package edu.upenn.diffstream.examples.topiccount;

import edu.upenn.diffstream.FullDependence;
import edu.upenn.diffstream.matcher.MatcherFactory;
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

public class TopicCountTest {

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

        env.setParallelism(1);

        DataStream<TopicCountItem> inStream = env.fromElements(TopicCountItem.class,
                new Word("a"), new Word("math"), new Word("larry"), new Word("boat"), new EndOfFile(),
                new Word("math"), new Word("a"), new Word("boat"), new Word("larry"), new EndOfFile());
        DataStream<String> outStreamSeq = new TopicCountSequential().apply(inStream);
        DataStream<String> outStreamPar = new TopicCountSequential(4).apply(inStream);
        try (var matcher =
                     MatcherFactory.createMatcher(outStreamSeq, outStreamPar, new FullDependence<>())) {
            env.execute();
            matcher.assertStreamsAreEquivalent();
        }
    }

    @Ignore
    @Test
    public void testWordCountParallel() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<TopicCountItem> inStream = env.fromElements(TopicCountItem.class,
                new Word("a"), new Word("math"), new Word("larry"), new Word("boat"), new EndOfFile(),
                new Word("math"), new Word("a"), new Word("boat"), new Word("larry"), new EndOfFile());
        DataStream<String> outStreamSeq = new TopicCountSequential().apply(inStream);
        DataStream<String> outStreamPar = new TopicCountParallel(4).apply(inStream);
        try (var matcher =
                     MatcherFactory.createMatcher(outStreamSeq, outStreamPar, new FullDependence<>())) {
            env.execute();
            matcher.assertStreamsAreEquivalent();
        }
    }

    @Ignore
    @Test
    public void testWordCountSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        int wordsPerDocument = 10;
        int totalDocuments = 10;
        DataStream<Integer> out = env.addSource(new TopicCountSource(wordsPerDocument, totalDocuments))
                .flatMap(new FlatMapFunction<TopicCountItem, Integer>() {
                    @Override
                    public void flatMap(TopicCountItem item, Collector<Integer> collector) {
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
        try (var matcher =
                     MatcherFactory.createMatcher(out, expected, new FullDependence<>())) {
            env.execute();
            matcher.assertStreamsAreEquivalent();
        }
    }

}
