package edu.upenn.streamstesting.examples.fold;

import edu.upenn.streamstesting.EmptyDependence;
import edu.upenn.streamstesting.SinkBasedMatcher;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class CountIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(CountIntegrationTest.class);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(4)
                            .setNumberTaskManagers(1)
                            .build());

    /**
     * In this test we want a Flink program that counts the number of elements in a stream.
     * We wanted to observe a similar behavior to the one in Spark described
     * <a href="http://www.russellspitzer.com/2015/11/25/Folding-in-Spark-On-The-Edge/">here</a>.
     * However, if we are working with a stream, we need to somehow trigger the fold, and
     * that is done by defining a window. Within a window, Flink doesn't parallelize, so the
     * result is as expected.
     *
     * @throws Exception
     */
    @Test
    public void testCount() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        final long numElements = 10;

        DataStream<Long> input = env.generateSequence(1, numElements);
        DataStream<Long> output = input
                .countWindowAll(numElements)
                .fold(0L, (acc, el) -> { LOG.info("acc: {} el: {}", acc, el); return acc + 1; });

        DataStream<Long> correctOutput = env.fromElements(numElements);

        SinkBasedMatcher<Long> matcher = SinkBasedMatcher.createMatcher(new EmptyDependence<>());
        output.addSink(matcher.getSinkLeft());
        correctOutput.addSink(matcher.getSinkRight());

        env.execute();

        assertTrue("The program should correctly count the number of elements", matcher.streamsAreEquivalent());
    }

    @Test
    public void testCountDataSet() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        final long numElements = 10;

        DataSet<Long> input = env.generateSequence(1, numElements);
        DataSet<Long> output = input.reduce((acc, el) -> { LOG.info("acc: {} el: {}", acc, el); return acc + 1; });

        output.print();
    }
}
