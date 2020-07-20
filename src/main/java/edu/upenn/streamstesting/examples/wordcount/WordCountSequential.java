package edu.upenn.streamstesting.examples.wordcount;

import edu.upenn.streamstesting.FlinkProgram;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class WordCountSequential implements FlinkProgram<WordCountItem, String> {

    private final int parallelism;

    public WordCountSequential() {
        this.parallelism = 1;
    }

    /**
     * Even though the operator is supposed to be sequential, we allow setting
     * parallelism to more than 1.
     *
     * @param parallelism
     */
    public WordCountSequential(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public DataStream<String> apply(DataStream<WordCountItem> inputStream) {
        return inputStream.flatMap(new RichFlatMapFunction<WordCountItem, String>() {
            private final Map<String, Integer> topicCount = new HashMap<>();
            private Jedis jedis = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = new Jedis(WordRepository.REDIS_HOST);
            }

            @Override
            public void flatMap(WordCountItem wordCountItem, Collector<String> collector) throws Exception {
                wordCountItem.<Void>match(
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
