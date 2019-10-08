package edu.upenn.streamstesting.examples.flinktraining;

import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.generator.InRange;
import edu.upenn.streamstesting.InputGenerator;
import edu.upenn.streamstesting.StreamEquivalenceMatcher;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;


// Open ended questions and TODOs

// TODO: Is it possible to modify parallelism of the operators programmaticaly?
//       Find the operators in the pipeline and set their parallelism to 1 or 2

// TODO: How can I make the sequential computation really be sequential?

// TODO: In order to have a proper story for continuous safe upgrade, we need to think about the debugging output
//       that the matcher outputs whenever equivalence is not true. After we think about this, we should implement
//       and write about it somewhere in the paper too, as it is part of the story.

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
    sequentialComputation(DataStream<Tuple3<@InRange(minLong = 0L, maxLong = 10L) Long, Tuple2<Long, Long>, Integer>> events) {

        // The input is supposed to be taxiId, position, and metadata.

        // We first project the position and taxiId
        SingleOutputStreamOperator<Tuple2<Long, Tuple2<Long, Long>>> positions = events.project(0, 1);
        positions.setParallelism(1);

        // Then we keyBy TaxiId
        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> positionsByTaxi = positions.keyBy(0);

        // TODO: Would it be possible to also setParallelism(1) here?

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

    public DataStream<Tuple2<Long, Tuple2<Long, Long>>>
    correctParallelComputation(DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>> events) {

        // The input is supposed to be taxiId, position, and metadata.

        KeyedStream<Tuple3<Long, Tuple2<Long, Long>, Integer>, Tuple> byTaxi = events.keyBy(0);

        DataStream<Tuple2<Long, Tuple2<Long, Long>>> positionsByTaxi = byTaxi.project(0, 1);

        // Then we Probe
        return positionsByTaxi;
    }

    // TODO: Make this more streamlined. Discuss how to do this
    public DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>> generateInput(StreamExecutionEnvironment env)
            throws NoSuchMethodException {

        // Note: All of the lines until the call to the parameterGenerator method, can be circumvented
        // if one knows exactly which generator they want. Then, they can just initialize it.
        InputGenerator<DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>>> inputGen =
                new InputGenerator(env);

        Class[] cArg = new Class[1];
        cArg[0] = DataStream.class;

        Method testMethod = getClass().getMethod("sequentialComputation", DataStream.class);

        Parameter parameter = testMethod.getParameters()[0];

        Generator<DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>>> generator =
                (Generator<DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>>>) inputGen.parameterGenerator(parameter);
        DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>> stream = inputGen.generate(generator);

        return stream;
    }



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

        StreamEquivalenceMatcher<Tuple2<Long, Tuple2<Long, Long>>> matcher = StreamEquivalenceMatcher.createMatcher(new KeyByParallelismDependence());
        seqOutput.addSink(matcher.getSinkLeft()).setParallelism(1);
        parallelOutput.addSink(matcher.getSinkRight()).setParallelism(1);

        env.execute();

        matcher.assertStreamsAreEquivalent();

    }

    @Test(expected = Exception.class)
    public void testPositionsByKeyInputGenerator() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO: Set the parallelism of the source to 1 and the parallelism of the operators to 2 or more.
        // TODO: In order to do this, we have to return a source instead of a datastream
        DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>> input = generateInput(env);

        input.print();

        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> seqOutput = sequentialComputation(input);
        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> parallelOutput = parallelComputation(input);

        // TODO: Make an input generator that generates keys from a specific range for one of the fields

        // Ideal interface for user: A way to specify for which field (or tuple element) of the items of the data stream
        // should the program only generate events inrange. I don't want all the internal components to generate events
        // in a specific range.
        //
        // Note: At the moment I am doing it with InRange annotation on the test itself. That is clearly not the best
        //       way to do that.
        //
        // Question: How can one specify what is the exact field for which to generate items in range, rather than
        //           arbitrary longs.


        StreamEquivalenceMatcher<Tuple2<Long, Tuple2<Long, Long>>> matcher = StreamEquivalenceMatcher.createMatcher(new KeyByParallelismDependence());
        seqOutput.addSink(matcher.getSinkLeft()).setParallelism(1);
        parallelOutput.addSink(matcher.getSinkRight()).setParallelism(1);

        env.execute();
    }

    @Test
    public void correctTestPositionsByKeyInputGenerator() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>> input = generateInput(env);

        input.print();

        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> seqOutput = sequentialComputation(input);
        DataStream<Tuple2<Long, Tuple2<Long, Long>>> parallelOutput = correctParallelComputation(input);


        StreamEquivalenceMatcher<Tuple2<Long, Tuple2<Long, Long>>> matcher = StreamEquivalenceMatcher.createMatcher(new KeyByParallelismDependence());
        seqOutput.addSink(matcher.getSinkLeft()).setParallelism(1);
        parallelOutput.addSink(matcher.getSinkRight()).setParallelism(1);

        env.execute();

        matcher.assertStreamsAreEquivalent();
    }


    @Test
    public void manualTestPositionsByKey() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Generating input is part of both scenarios, however in our case it should be much simpler to do.
        DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>> input = generateInput(env);

        input.print();

        // Note: In my opinion, we should contrast differential testing with unit and integration testing.
        //       So here we should have both the case where the user predicts the possible output,
        //       making a matcher that checks the correct output against that, and (maybe) also a case
        //       where the user writes a sequential and a parallel computation and compares them.
        //
        // Note: In the case of manual integration/unit testing, one cannot even use a random generator,
        //       because they would have to make the output by hand.
        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> seqOutput = sequentialComputation(input);
        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> parallelOutput = parallelComputation(input);

        seqOutput.addSink(new KeyByParallelismManualSink(true, false)).setParallelism(1);
        parallelOutput.addSink(new KeyByParallelismManualSink(false, false)).setParallelism(1);

        env.execute();

        assertFalse("The two implementations should have unmatched items :)", KeyByParallelismManualMatcher.allMatched());
    }

}
