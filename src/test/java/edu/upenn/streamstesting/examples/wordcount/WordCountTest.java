package edu.upenn.streamstesting.examples.wordcount;

import edu.upenn.streamstesting.examples.flinktraining.WindowsTest;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class WordCountTest {
    private static final Logger LOG = LoggerFactory.getLogger(WindowsTest.class);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void redisTest () {
        Jedis jedis = new Jedis("localhost");
        jedis.set("foo", "bar");
        String value = jedis.get("foo");
        System.out.println("Value for foo: " + value);
        assert(value.equals("bar"));
    }

    @Test
    public void redisTest2 () {
        Jedis jedis = new Jedis("localhost");
        WordCount wc = new WordCount(jedis);
        wc.fillJedisManual();
        String value = jedis.get("haskell");
        assert(value.equals("science"));
        String value2 = jedis.get("java");
        assert(value2.equals("bean"));
    }

    @Test
    public void manualWordCountTest() throws Exception {

        // Initialize and fill Jedis
        WordCount wc = new WordCount();
        wc.fillJedisManual();

        // Initialize the FLink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.fromElements("haskell", "science");

        DataStream<Tuple2<String, String>> output = wc.sequentialComputation(source);

        output.print();

        env.execute();
    }
}
