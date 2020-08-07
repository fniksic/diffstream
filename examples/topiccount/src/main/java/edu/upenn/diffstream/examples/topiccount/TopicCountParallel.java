package edu.upenn.diffstream.examples.topiccount;

import edu.upenn.diffstream.FlinkProgram;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TopicCountParallel implements FlinkProgram<TopicCountItem, String>, Serializable {

    private static final long serialVersionUID = 7748747289925717465L;

    private final int parallelism;

    public TopicCountParallel(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public DataStream<String> apply(DataStream<TopicCountItem> inputStream) {
        return inputStream.assignTimestampsAndWatermarks(new EndOfFileToWatermarkAssigner()).setParallelism(1)
                .flatMap(new FlatMapFunction<TopicCountItem, Word>() {
                    @Override
                    public void flatMap(TopicCountItem item, Collector<Word> collector) {
                        item.<Void>match(
                                word -> {
                                    collector.collect(word);
                                    return null;
                                },
                                endOfFile -> null
                        );
                    }
                })
                .map(new RichMapFunction<Word, Tuple2<Word, String>>() {
                    Jedis jedis = null;

                    @Override
                    public void open(Configuration parameters) {
                        jedis = new Jedis(WordRepository.REDIS_HOST);
                    }

                    @Override
                    public Tuple2<Word, String> map(Word word) {
                        String topic = WordRepository.lookupTopic(jedis, word.getWord());
                        return Tuple2.of(word, topic);
                    }
                })
                .keyBy(word -> word.hashCode() % parallelism)
                .timeWindow(Time.milliseconds(1L))
                .aggregate(new TopicAggregator())
                .timeWindowAll(Time.milliseconds(1L))
                .reduce(TopicCountParallel::mergeTopicCounts)
                .map(TopicCountParallel::mostFrequentTopic);
    }

    private static Map<String, Integer> mergeTopicCounts(Map<String, Integer> topicCount1,
                                                         Map<String, Integer> topicCount2) {
        for (Map.Entry<String, Integer> entry : topicCount2.entrySet()) {
            int newCount = topicCount1.getOrDefault(entry.getKey(), 0) + entry.getValue();
            topicCount1.put(entry.getKey(), newCount);
        }
        return topicCount1;
    }

    private static String mostFrequentTopic(Map<String, Integer> topicCount) {
        String topic = null;
        int maxCount = 0;
        for (Map.Entry<String, Integer> entry : topicCount.entrySet()) {
            if (entry.getValue() > maxCount) {
                topic = entry.getKey();
                maxCount = entry.getValue();
            }
        }
        return topic;
    }

    private static class EndOfFileToWatermarkAssigner implements AssignerWithPunctuatedWatermarks<TopicCountItem> {
        private long eofCount = 0L;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(TopicCountItem item, long extractedTimestamp) {
            return item.match(
                    word -> null,
                    endOfFile -> {
                        eofCount++;
                        return new Watermark(extractedTimestamp);
                    }
            );
        }

        @Override
        public long extractTimestamp(TopicCountItem item, long previousElementTimestamp) {
            return eofCount;
        }
    }

    private static class TopicAggregator implements AggregateFunction<Tuple2<Word, String>, Map<String, Integer>, Map<String, Integer>> {
        @Override
        public Map<String, Integer> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<String, Integer> add(Tuple2<Word, String> pair, Map<String, Integer> topicCount) {
            int newCount = topicCount.getOrDefault(pair.f1, 0) + 1;
            topicCount.put(pair.f1, newCount);
            return topicCount;
        }

        @Override
        public Map<String, Integer> getResult(Map<String, Integer> topicCount) {
            return topicCount;
        }

        @Override
        public Map<String, Integer> merge(Map<String, Integer> topicCount1, Map<String, Integer> topicCount2) {
            return mergeTopicCounts(topicCount1, topicCount2);
        }
    }
}
