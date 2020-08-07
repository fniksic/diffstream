package edu.upenn.diffstream.remote;

import edu.upenn.diffstream.Dependence;
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

        DataStream<Integer> first = env.fromElements(1, 2, 0, 3, 1, 0, 2).setParallelism(1);
        DataStream<Integer> second = env.fromElements(2, 1, 0, 1, 3, 0).setParallelism(1);

        RemoteMatcherFactory.init();
        RemoteStreamEquivalenceMatcher<Integer> matcher = RemoteMatcherFactory.getInstance().
                createMatcher(first, second, new DifferentSign());
        env.execute();
        matcher.assertStreamsAreEquivalent();
        RemoteMatcherFactory.destroy();
    }

    @Ignore
    @Test
    public void testRemoteIncDecEquivalent() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStream<Integer> first = env.fromElements(1, 2, 0, 3, 1, 0).setParallelism(1);
        DataStream<Integer> second = env.fromElements(2, 1, 0, 1, 3, 0).setParallelism(1);

        RemoteMatcherFactory.init();
        RemoteStreamEquivalenceMatcher<Integer> matcher = RemoteMatcherFactory.getInstance().
                createMatcher(first, second, new DifferentSign());
        env.execute();
        matcher.assertStreamsAreEquivalent();
        RemoteMatcherFactory.destroy();
    }

    @Ignore
    @Test
    public void testRemoteIncDecEquivalentLogUnmatched() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStream<Integer> first = env.fromElements(1, 2, 0, 3, 1, 0).setParallelism(1);
        DataStream<Integer> second = env.fromElements(2, 1, 0, 1, 3, 0).setParallelism(1);

        RemoteMatcherFactory.init();
        RemoteStreamEquivalenceMatcher<Integer> matcher = RemoteMatcherFactory.getInstance().
                createMatcher(first, second, new DifferentSign(), true);
        env.execute();
        matcher.assertStreamsAreEquivalent();
        RemoteMatcherFactory.destroy();
    }

    private static class DifferentSign implements Dependence<Integer> {

        @Override
        public boolean test(final Integer fst, final Integer snd) {
            return !((fst < 0 && snd < 0) || (fst > 0 && snd > 0));
        }

    }
}
