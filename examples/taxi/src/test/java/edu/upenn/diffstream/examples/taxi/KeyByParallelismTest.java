package edu.upenn.diffstream.examples.taxi;

import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.generator.InRange;
import edu.upenn.diffstream.InputGenerator;
import edu.upenn.diffstream.StreamEquivalenceMatcher;
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

import static org.junit.Assert.assertFalse;


public class KeyByParallelismTest {

    private static final Logger LOG = LoggerFactory.getLogger(KeyByParallelismTest.class);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    public KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple>
    sequentialComputation(DataStream<Tuple3<@InRange(minLong = 0L, maxLong = 10L) Long, Tuple2<Long, Long>, Integer>> events) {

        // The input is supposed to be taxiID, position, and metadata.

        // We first project the taxiID and position
        SingleOutputStreamOperator<Tuple2<Long, Tuple2<Long, Long>>> positions = events.project(0, 1);
        positions.setParallelism(1);

        // Then we keyBy TaxiID
        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> positionsByTaxi = positions.keyBy(0);

        // Then we Probe
        return positionsByTaxi;
    }

    public KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple>
    parallelComputation(DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>> events) {

        // The input is supposed to be taxiID, position, and metadata.

        // We first project the taxiID and position
        DataStream<Tuple2<Long, Tuple2<Long, Long>>> positions = events.project(0, 1);

        // Then we keyBy TaxiID
        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> positionsByTaxi = positions.keyBy(0);

        // Then we Probe
        return positionsByTaxi;
    }

    public DataStream<Tuple2<Long, Tuple2<Long, Long>>>
    correctParallelComputation(DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>> events) {

        // The input is supposed to be taxiID, position, and metadata.

        KeyedStream<Tuple3<Long, Tuple2<Long, Long>, Integer>, Tuple> byTaxi = events.keyBy(0);

        DataStream<Tuple2<Long, Tuple2<Long, Long>>> positionsByTaxi = byTaxi.project(0, 1);

        // Then we Probe
        return positionsByTaxi;
    }

    public DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>> generateInput(StreamExecutionEnvironment env)
            throws NoSuchMethodException {

        // Note: All of the lines until the call to the parameterGenerator method, can be circumvented
        // if one knows exactly which generator they want. Then, they can just initialize it.
        InputGenerator<DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>>> inputGen =
                new InputGenerator<>(env);

        Method testMethod = getClass().getMethod("sequentialComputation", DataStream.class);

        Parameter parameter = testMethod.getParameters()[0];

        Generator<DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>>> generator =
                (Generator<DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>>>) inputGen.parameterGenerator(parameter);

        return inputGen.generate(generator);
    }



    @Test(expected = Exception.class)
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

        DataStream<Tuple3<Long, Tuple2<Long, Long>, Integer>> input = generateInput(env);

        input.print();

        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> seqOutput = sequentialComputation(input);
        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> parallelOutput = parallelComputation(input);

        StreamEquivalenceMatcher<Tuple2<Long, Tuple2<Long, Long>>> matcher =
                StreamEquivalenceMatcher.createMatcher(seqOutput, parallelOutput, (fst, snd) -> fst.f0.equals(snd.f0));

        env.execute();

        matcher.assertStreamsAreEquivalent();
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
        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> seqOutput = sequentialComputation(input);
        KeyedStream<Tuple2<Long, Tuple2<Long, Long>>, Tuple> parallelOutput = parallelComputation(input);

        seqOutput.addSink(new KeyByParallelismManualSink(true, false)).setParallelism(1);
        parallelOutput.addSink(new KeyByParallelismManualSink(false, false)).setParallelism(1);

        env.execute();

        assertFalse("The two implementations should have unmatched items :)", KeyByParallelismManualMatcher.allMatched());
    }

}
