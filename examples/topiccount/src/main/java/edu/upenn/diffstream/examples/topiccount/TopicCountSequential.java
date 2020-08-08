package edu.upenn.diffstream.examples.topiccount;

import edu.upenn.diffstream.FlinkProgram;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class TopicCountSequential implements FlinkProgram<TopicCountItem, String> {

    private final int parallelism;

    public TopicCountSequential() {
        this.parallelism = 1;
    }

    /**
     * Even though the operator is supposed to be sequential, we allow setting
     * parallelism to more than 1.
     *
     * @param parallelism
     */
    public TopicCountSequential(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public DataStream<String> apply(DataStream<TopicCountItem> inputStream) {
        return inputStream.flatMap(new RichFlatMapFunction<TopicCountItem, String>() {
            private final Map<String, Integer> topicCount = new HashMap<>();
            private Jedis jedis = null;

            @Override
            public void open(Configuration parameters) {
                jedis = new Jedis(WordRepository.REDIS_HOST);
            }

            @Override
            public void flatMap(TopicCountItem topicCountItem, Collector<String> collector) {
                topicCountItem.<Void>match(
                        word -> {
                            String topic = WordRepository.lookupTopic(jedis, word.getWord());
                            int newCount = 1 + topicCount.getOrDefault(topic, 0);
                            topicCount.put(topic, newCount);
                            return null;
                        },
                        endOfFile -> {
                            String documentTopic = null;
                            int maxCount = 0;
                            for (Map.Entry<String, Integer> entry : topicCount.entrySet()) {
                                if (entry.getValue() > maxCount) {
                                    documentTopic = entry.getKey();
                                    maxCount = entry.getValue();
                                }
                            }
                            topicCount.clear();
                            collector.collect(documentTopic);
                            return null;
                        }
                );
            }
        }).setParallelism(parallelism);
    }

}
