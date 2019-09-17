package edu.upenn.streamstesting.examples.incdec;

import edu.upenn.streamstesting.Matcher;
import edu.upenn.streamstesting.MatcherSink;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class IncDecIntegrationTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Ignore
    @Test
    public void testIncDec() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MatcherSink sink = new MatcherSink();

        DataStream<IncDecItem> first = env.fromElements(IncDecItem.class,
                new Inc(1), new Dec(2), new Hash(),
                new Inc(3), new Dec(1), new Hash()
        );
        DataStream<IncDecItem> second = env.fromElements(IncDecItem.class,
                new Inc(2), new Dec(1), new Hash(),
                new Inc(1), new Dec(3), new Hash()
        );

        first.connect(second).flatMap(new Matcher<>(new IncDecDependence())).addSink(sink);

        env.execute();

        assertTrue("The two IncDec data streams should be equivalent", sink.equalsTrue());
    }
}
