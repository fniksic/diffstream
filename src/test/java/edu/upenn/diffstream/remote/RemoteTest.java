package edu.upenn.diffstream.remote;

import edu.upenn.diffstream.Dependence;
import edu.upenn.diffstream.matcher.MatcherFactory;
import edu.upenn.diffstream.matcher.StreamsNotEquivalentException;
import edu.upenn.diffstream.monitor.ResourceMonitor;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;

@Ignore
public class RemoteTest {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteTest.class);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    public static ResourceMonitor monitor =
            new ResourceMonitor("unmatched-items.txt", "memory-log.txt");

    @BeforeClass
    public static void initRemote() throws RemoteException {
        MatcherFactory.initRemote();
        monitor.start();
    }

    @AfterClass
    public static void destroyRemote() throws RemoteException {
        monitor.close();
        MatcherFactory.destroyRemote();
    }

    @Test(expected = StreamsNotEquivalentException.class)
    public void testRemoteIncDecNonEquivalent() throws StreamsNotEquivalentException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        runTest(env, env.fromElements(1, 2, 0, 3, 1, 0, 2), env.fromElements(2, 1, 0, 1, 3, 0));
    }

    @Test
    public void testRemoteIncDecEquivalent() throws StreamsNotEquivalentException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        runTest(env, env.fromElements(1, 2, 0, 3, 1, 0), env.fromElements(2, 1, 0, 1, 3, 0));
    }

    @Test
    public void testRemoteIncDecEquivalentLogUnmatched() throws StreamsNotEquivalentException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        runTest(env, env.fromElements(1, 2, 0, 3, 1, 0), env.fromElements(2, 1, 0, 1, 3, 0));
    }

    private void runTest(StreamExecutionEnvironment env,
                         DataStream<Integer> first,
                         DataStream<Integer> second) throws StreamsNotEquivalentException {
        env.setParallelism(1);
        try (var matcher = MatcherFactory.createRemoteMatcher(first, second, new DifferentSign());
             AutoCloseable ignored = () -> monitor.unobserve(matcher)) {
            monitor.observe(matcher);
            env.execute();
            matcher.assertStreamsAreEquivalent();
        } catch (Exception e) {
            if (StreamsNotEquivalentException.isRootCauseOf(e)) {
                throw new StreamsNotEquivalentException();
            }
            LOG.error("Test not executed due to exception", e);
        }
    }

    private static class DifferentSign implements Dependence<Integer> {

        @Override
        public boolean test(final Integer fst, final Integer snd) {
            return !((fst < 0 && snd < 0) || (fst > 0 && snd > 0));
        }

    }

}
