package edu.upenn.diffstream.tutorial.incdec;

import edu.upenn.diffstream.StreamEquivalenceMatcher;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * A test class with several IncDec tests, demonstrating how {@link StreamEquivalenceMatcher}
 * can be used to test equivalence of manually provided streams.
 */
public class IncDecTest {

    /**
     * Flink provides a mini cluster that is suitable for testing purposes. We
     * configure the cluster to have 1 task manager with 2 task slots, which allows
     * a total parallelism of 2. We need parallelism of at least 2 if we want the
     * stream equivalence matcher to match two streams in parallel.
     */
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    /**
     * The first test uses {@link StreamEquivalenceMatcher} to establish non-equivalence
     * of two manually provided streams. The non-equivalence is established by the matcher
     * throwing an exception. We state that this is expected in the @Test annotation.
     *
     * @throws Exception
     */
    @Test(expected = Exception.class)
    public void testIncDecNonEquivalent() throws Exception {

        // We first get an execution environment on the mini cluster
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // By setting parallelism to 1, we state that every operator, which includes
        // sources and sinks, should only have a single instance. This is what we want: even though
        // we want the matcher to match the two streams defined below in parallel, we don't want
        // Flink to parallelize the streams themselves, since that would destroy the order of events
        // in them.
        env.setParallelism(1);

        // We manually define two streams, first and second. Note that the streams are not
        // equivalent with respect to IncDecDependence (see the comments in the description of
        // that class for more information):
        // even though the streams contain the same increments, decrements, and barriers,
        // there is no valid reordering of the first stream that produces the second stream.
        DataStream<IncDecItem> first = env.fromElements(IncDecItem.class,
                new Inc(1), new Dec(2), new Barrier(),
                new Inc(3), new Dec(1), new Barrier()
        );
        DataStream<IncDecItem> second = env.fromElements(IncDecItem.class,
                new Dec(2), new Inc(1), new Barrier(),
                new Dec(1), new Inc(3), new Barrier()
        );

        // We create a matcher for the two streams, with and instance of IncDecDependence
        // dependence relation.
        StreamEquivalenceMatcher<IncDecItem> matcher =
                StreamEquivalenceMatcher.createMatcher(first, second, new IncDecDependence());

        // With the two manually defined streams serving as sources and the matcher serving
        // as sinks, we have defined a Flink program. We execute it on the execution
        // environment.
        env.execute();

        // We assert that the streams are equivalent. Since this is not the case,
        // the assertion will throw an exception, as expected by our test case.
        matcher.assertStreamsAreEquivalent();
    }

    /**
     * In the second test we establish equivalence of two manually provided streams.
     *
     * @throws Exception
     */
    @Test
    public void testIncDecEquivalent() throws Exception {

        // As before, we get an execution environment on the mini cluster and
        // set parallelism to 1.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // This time the two streams are equivalent according to IncDecDependence (see the
        // comments in the description of that class for more information):
        // the decrements and the increments can be reordered in the first stream to
        // obtain the second stream.
        DataStream<IncDecItem> first = env.fromElements(IncDecItem.class,
                new Dec(1), new Dec(2), new Barrier(),
                new Inc(3), new Inc(1), new Barrier()
        );
        DataStream<IncDecItem> second = env.fromElements(IncDecItem.class,
                new Dec(2), new Dec(1), new Barrier(),
                new Inc(1), new Inc(3), new Barrier()
        );

        // We create a matcher for the two streams with an instance of IncDecDependence
        // dependence relation. We execute the Flink program on the execution environment
        // and assert equivalence of the two streams.
        StreamEquivalenceMatcher<IncDecItem> matcher =
                StreamEquivalenceMatcher.createMatcher(first, second, new IncDecDependence());
        env.execute();
        matcher.assertStreamsAreEquivalent();
    }
}
