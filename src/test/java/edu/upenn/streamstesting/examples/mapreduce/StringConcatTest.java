package edu.upenn.streamstesting.examples.mapreduce;

import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.generator.Size;
import edu.upenn.streamstesting.EmptyDependence;
import edu.upenn.streamstesting.FullDependence;
import edu.upenn.streamstesting.InputGenerator;
import edu.upenn.streamstesting.StreamEquivalenceMatcher;
import edu.upenn.streamstesting.examples.flinktraining.WindowsTest;
import edu.upenn.streamstesting.generators.WithTimestamps;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class StringConcatTest {

    private static final Logger LOG = LoggerFactory.getLogger(WindowsTest.class);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    private static Tuple2<Integer, String> id(Tuple2<Integer, String> x) {
        return x;
    }

    private static KeyedString idToKeyString(Tuple2<Integer, String> x) {
        Integer first = x.f0;
        String second = x.f1;
        return new KeyedString(first, second);
    }

    // This class exists to define the custom dependence relation that checks for equivalence
    // by considering string to be sets of strings concatenated with "|"
    private static class KeyedString extends Tuple2<Integer, String> {

        public KeyedString(Integer f0, String f1) {
            super(f0, f1);
        }

        public boolean equals(KeyedString other) {
            boolean keysEq = this.f0 == other.f0;
            HashSet<String> thisSet = new HashSet<>(Arrays.asList(this.f1.split("@")));
            HashSet<String> otherSet = new HashSet<>(Arrays.asList(other.f1.split("@")));
            return keysEq && (thisSet.equals(otherSet));
        }
    }

    // This function takes a stream of keyed strings and concatenates the strings for each key.
    // A test around this computation can ignore the false positive if the equivalence relation is set to check the
    // strings for equivalence as a set of strings concatenated with |.
    public SingleOutputStreamOperator<KeyedString> concatenate(
            @WithTimestamps @Size(min=10000, max=100000) DataStream<Tuple2<Integer, String>> events, Boolean isSequential) {

        SingleOutputStreamOperator<KeyedString> mapped = events
                .map(StringConcatTest::idToKeyString);

        if(isSequential) {
            mapped.setParallelism(1);
        } else {
            mapped.setParallelism(2);
        }

        KeyedStream<KeyedString, Tuple> keyed = mapped
                .keyBy("f0");

        SingleOutputStreamOperator<KeyedString> concat = keyed
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                        .reduce(new StrConcatReducer());

        if(isSequential) {
            concat.setParallelism(1);
        } else {
            concat.setParallelism(2);
        }

        return concat;
    }

    // This is a reducer that concatenates all the strings in a window to one string
    // In this case the tester should have a different equivalence relation.
    public class StrConcatReducer implements ReduceFunction<KeyedString>
    {
        @Override
        public KeyedString reduce(KeyedString in1, KeyedString in2) {
            return new KeyedString(in1.f0, in1.f1 + "@" + in2.f1);
        }
    }

    // This function splits the stream by key (as would the first step of a reducer do.
    // Then we can compute the next steps (either order dependent or independent)
    public KeyedStream<Tuple2<Integer, String>, Tuple> preConcatenate(
            DataStream<Tuple2<@InRange(minInt = 0, maxInt = 10)Integer, String>> events, Boolean isSequential) {
        // Identity map to break the order
        SingleOutputStreamOperator<Tuple2<Integer, String>> mapped = events
                .map(StringConcatTest::id);

        if(isSequential) {
            mapped.setParallelism(1);
        } else {
            mapped.setParallelism(2);
        }

        KeyedStream<Tuple2<Integer, String>, Tuple> keyed = mapped
                .keyBy(0);
        return keyed;
    }


    public DataStream<Tuple2<Integer, String>> generateInput(StreamExecutionEnvironment env, String methodName)
            throws NoSuchMethodException {

        // Note: All of the lines until the call to the parameterGenerator method, can be circumvented
        // if one knows exactly which generator they want. Then, they can just initialize it.
        InputGenerator<DataStream<Tuple2<Integer, String>>> inputGen =
                new InputGenerator(env);

        Class[] cArg = new Class[1];
        cArg[0] = DataStream.class;

        Method testMethod = getClass().getMethod(methodName, DataStream.class, Boolean.class);

        Parameter parameter = testMethod.getParameters()[0];

        Generator<DataStream<Tuple2<Integer, String>>> generator =
                (Generator<DataStream<Tuple2<Integer, String>>>) inputGen.parameterGenerator(parameter);
        DataStream<Tuple2<Integer, String>> stream = inputGen.generate(generator);

        return stream;
    }

    // This and the following tests are what would happen if there is order dependence and independence
    // expected by the downstream operator after the keyBy. The keyBy essentially is the interesting
    // part of the map reduce example string-concat. The 5th bug would be caught like this, if the
    // users specified that the downstream method expects ordered input by key.
    @Test(expected = Exception.class)
    public void testOrderKeyDependent() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // WARNING: It might be the case that the input generates the character '|' in the strings, which
        // could lead to problems. TODO: Fix that
        DataStream<Tuple2<Integer, String>> input = generateInput(env, "preConcatenate");

        KeyedStream<Tuple2<Integer, String>, Tuple> seqOutput = preConcatenate(input, true);
        KeyedStream<Tuple2<Integer, String>, Tuple> parallelOutput = preConcatenate(input, false);


        StreamEquivalenceMatcher matcher =
                StreamEquivalenceMatcher.createMatcher(seqOutput, parallelOutput, ((fst, snd) -> fst.f0 == snd.f0));

        env.execute();
    }

    @Test
    public void testOrderIndependent() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer, String>> input = generateInput(env, "preConcatenate");

        KeyedStream<Tuple2<Integer, String>, Tuple> seqOutput = preConcatenate(input, true);
        KeyedStream<Tuple2<Integer, String>, Tuple> parallelOutput = preConcatenate(input, false);


        StreamEquivalenceMatcher matcher =
                StreamEquivalenceMatcher.createMatcher(seqOutput, parallelOutput, new EmptyDependence<>());

        env.execute();
        matcher.assertStreamsAreEquivalent();
    }

    @Ignore
    public void testConcat() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<Integer, String>> input = generateInput(env, "concatenate");

        SingleOutputStreamOperator<KeyedString> seqOutput = concatenate(input, true);
        SingleOutputStreamOperator<KeyedString> parallelOutput = concatenate(input, false);


        StreamEquivalenceMatcher matcher =
                StreamEquivalenceMatcher.createMatcher(seqOutput, parallelOutput, new FullDependence<>());

        env.execute();
        matcher.assertStreamsAreEquivalent();
    }

}
