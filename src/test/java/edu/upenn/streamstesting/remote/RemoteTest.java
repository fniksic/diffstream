package edu.upenn.streamstesting.remote;

import edu.upenn.streamstesting.StreamEquivalenceMatcher;
import edu.upenn.streamstesting.examples.incdec.*;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class RemoteTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Ignore
    @Test(expected = Exception.class)
    public void testRemoteIncDecNonEquivalent() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStream<IncDecItem> first = env.fromElements(IncDecItem.class,
                new Dec(1), new Dec(2), new Hash(),
                new Inc(3), new Inc(1), new Hash(), new Inc(2)
        ).setParallelism(1);
        DataStream<IncDecItem> second = env.fromElements(IncDecItem.class,
                new Dec(2), new Dec(1), new Hash(),
                new Inc(1), new Inc(3), new Hash()
        ).setParallelism(1);

        RemoteMatcherFactory.init();
        RemoteStreamEquivalenceMatcher<IncDecItem> matcher = RemoteMatcherFactory.getInstance().
                createMatcher(first, second, new IncDecDependence());
        env.execute();
        matcher.assertStreamsAreEquivalent();
        RemoteMatcherFactory.destroy();
    }

    @Ignore
    @Test
    public void testRemoteIncDecEquivalent() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStream<IncDecItem> first = env.fromElements(IncDecItem.class,
                new Dec(1), new Dec(2), new Hash(),
                new Inc(3), new Inc(1), new Hash()
        ).setParallelism(1);
        DataStream<IncDecItem> second = env.fromElements(IncDecItem.class,
                new Dec(2), new Dec(1), new Hash(),
                new Inc(1), new Inc(3), new Hash()
        ).setParallelism(1);

        RemoteMatcherFactory.init();
        RemoteStreamEquivalenceMatcher<IncDecItem> matcher = RemoteMatcherFactory.getInstance().
                createMatcher(first, second, new IncDecDependence());
        env.execute();
        matcher.assertStreamsAreEquivalent();
        RemoteMatcherFactory.destroy();
    }
}
