package edu.upenn.diffstream.tutorial.incdec;

import edu.upenn.diffstream.StreamEquivalenceMatcher;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

public class IncDecTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Test(expected = Exception.class)
    public void testIncDecNonEquivalent() throws Exception {
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

        StreamEquivalenceMatcher<IncDecItem> matcher = StreamEquivalenceMatcher.createMatcher(new IncDecDependence());
        first.addSink(matcher.getSinkLeft()).setParallelism(1);
        second.addSink(matcher.getSinkRight()).setParallelism(1);

        env.execute();

        matcher.assertStreamsAreEquivalent();
    }

    @Test
    public void testIncDecEquivalent() throws Exception {
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

        StreamEquivalenceMatcher<IncDecItem> matcher = StreamEquivalenceMatcher.createMatcher(new IncDecDependence());
        first.addSink(matcher.getSinkLeft()).setParallelism(1);
        second.addSink(matcher.getSinkRight()).setParallelism(1);

        env.execute();

        matcher.assertStreamsAreEquivalent();
    }
}
