package edu.upenn.streamstesting.examples.wordcount;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WordCount {
    private Jedis jedis;

    public WordCount() {
        jedis = new Jedis("localhost");
    }

    public WordCount(Jedis jedis) {
        this.jedis = jedis;
    }

    public void fillJedisManual() {
        // TODO: Do it by reading from a file
        List<Tuple2<String, String>> list =
                new ArrayList<>(Arrays.asList(
                        new Tuple2<>("haskell", "science"),
                        new Tuple2<>("java", "bean")
                ));
        fillJedis(list);
    }

    public void fillJedis(List<Tuple2<String, String>> pairs) {
        for (Tuple2<String, String> pair : pairs) {
            jedis.set(pair.f0, pair.f1);
        }
    }

    public DataStream<Tuple2<String, String>>
    sequentialComputation(DataStream<String> events) {

        // The input is words. For simplicity we can have end of page (or section) to be the empty string.

        DataStream<Tuple2<String, String>> output = events.flatMap(new RedisJoinBolt());

        // Then we Probe
        return output;
    }

    // Filters the words that have a topic and joins them with their topic.
    public static final class RedisJoinBolt extends RichFlatMapFunction<String, Tuple2<String, String>> {

        Jedis jedis;

        @Override
        public void open(Configuration parameters) {
            //initialize jedis
            this.jedis = new Jedis("localhost");
        }

        @Override
        public void flatMap(String input,
                            Collector<Tuple2<String, String>> out) throws Exception {
            String topic = jedis.get(input);
            if(topic == null) {
                return;
            }

            Tuple2<String, String> tuple = new Tuple2<>(input, topic);
            out.collect(tuple);
        }
    }
}
