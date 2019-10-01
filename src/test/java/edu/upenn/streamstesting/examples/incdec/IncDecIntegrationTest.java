package edu.upenn.streamstesting.examples.incdec;

import edu.upenn.streamstesting.SinkBasedMatcher;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IncDecIntegrationTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testIncDecNonEquivalent() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStream<IncDecItem> first = env.fromElements(IncDecItem.class,
                new Dec(1), new Dec(2), new Hash(),
                new Inc(3), new Inc(1), new Hash(), new Inc(2)
        );
        DataStream<IncDecItem> second = env.fromElements(IncDecItem.class,
                new Dec(2), new Dec(1), new Hash(),
                new Inc(1), new Inc(3), new Hash()
        );

        SinkBasedMatcher<IncDecItem> matcher = SinkBasedMatcher.createMatcher(new IncDecDependence());
        first.addSink(matcher.getSinkLeft()).setParallelism(1);
        second.addSink(matcher.getSinkRight()).setParallelism(1);

        env.execute();

        assertFalse("The two IncDec data streams should NOT be equivalent", matcher.streamsAreEquivalent());
    }

    @Test
    public void testIncDecEquivalent() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStream<IncDecItem> first = env.fromElements(IncDecItem.class,
                new Dec(1), new Dec(2), new Hash(),
                new Inc(3), new Inc(1), new Hash()
        );
        DataStream<IncDecItem> second = env.fromElements(IncDecItem.class,
                new Dec(2), new Dec(1), new Hash(),
                new Inc(1), new Inc(3), new Hash()
        );

        SinkBasedMatcher<IncDecItem> matcher = SinkBasedMatcher.createMatcher(new IncDecDependence());
        first.addSink(matcher.getSinkLeft()).setParallelism(1);
        second.addSink(matcher.getSinkRight()).setParallelism(1);

        env.execute();

        assertTrue("The two IncDec data streams should be equivalent", matcher.streamsAreEquivalent());
    }
}
