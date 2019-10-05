package edu.upenn.streamstesting.examples.flinktraining;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import edu.upenn.streamstesting.FullDependence;
import edu.upenn.streamstesting.StreamEquivalenceMatcher;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;

public class KeyByParallelismTest {

    private static final Logger LOG = LoggerFactory.getLogger(WindowsTest.class);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    public KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple>
    sequentialComputation(DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>> events) {

        // The input is supposed to be taxiId, position, and metadata.

        // We first project the position and taxiId
        DataStream<Tuple2<Long, Tuple2<Long, Long>>> positions = events.project(0, 1);

        // Then we keyBy TaxiId
        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> positionsByTaxi = positions.keyBy(0);

        // Then we Probe
        return positionsByTaxi;
    }

    public KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple>
    parallelComputation(DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>> events) {

        // The input is supposed to be taxiId, position, and metadata.

        // We first project the position and taxiId
        DataStream<Tuple2<Long, Tuple2<Long, Long>>> positions = events.project(0, 1);

        // Then we keyBy TaxiId
        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> positionsByTaxi = positions.keyBy(0);

        // Then we Probe
        return positionsByTaxi;
    }

    //@Ignore
    @Test
    public void testPositionsByKey() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Tuple3<Long, Tuple2<Long, Long>, Integer> el1 = new Tuple3<>(1L, new Tuple2<>(5L, 5L), 42);
        Tuple3<Long, Tuple2<Long, Long>, Integer> el2 = new Tuple3<>(1L, new Tuple2<>(10L, 10L), 42);
        Tuple3<Long, Tuple2<Long, Long>, Integer> el3 = new Tuple3<>(1L, new Tuple2<>(5L, 5L), 42);
        Tuple3<Long, Tuple2<Long, Long>, Integer> el4 = new Tuple3<>(1L, new Tuple2<>(0L, 0L), 42);

        DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>> input = env.fromElements(el1, el2, el3, el4);

        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> seqOutput = sequentialComputation(input);

        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> parallelOutput = parallelComputation(input);

        // TODO: Is it possible to modify parallelism of the operators programmaticaly?
        //       Find the operators in the pipeline and set their parallelism to 1 or 2

        // TODO: How can I make the sequential computation really be sequential?

        // TODO: Call the input generator instead of giving input by hand. In this case it fails
        //       even if given input by hand, but I have to make the generator work.

        StreamEquivalenceMatcher<Tuple2<Long, Tuple2<Long, Long>>> matcher = StreamEquivalenceMatcher.createMatcher(new FullDependence<>());
        seqOutput.addSink(matcher.getSinkLeft()).setParallelism(1);
        parallelOutput.addSink(matcher.getSinkRight()).setParallelism(1);

//        output.print();

        env.execute();

        assertFalse("The two implementations should be equivalent", matcher.streamsAreEquivalent());

    }


}
