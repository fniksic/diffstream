package edu.upenn.streamstesting.examples.mapreduce;

import com.pholser.junit.quickcheck.generator.Generator;
import edu.upenn.streamstesting.*;
import edu.upenn.streamstesting.examples.flinktraining.WindowsTest;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

public class MapReduceNondeterminismTest {

    /* =============== BOILERPLATE TO HELP RUN THE TESTS =============== */

    private static final Logger LOG = LoggerFactory.getLogger(WindowsTest.class);

    private static final int PARALLELISM = 2;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());


    // We use a map function which performs no computation, but may shuffle the order of events between partitions.
    // Then it partitions the (now nondeterministically ordered) stream by key.
    // This allows us to test if a particular reducer is correct on nondeterministically ordered input data.
    public KeyedStream<ReducerExamplesItem, Integer> identityMapper(
            DataStream<ReducerExamplesItem> events, Boolean isSequential) {

        // Stage 1: identity map. If not sequential, this should break the input order
        int parallelism = isSequential ? 1 : PARALLELISM;
        DataStream<ReducerExamplesItem> mapped = events
                .map(x -> x)
                .setParallelism(parallelism);

        // Stage 2: re-partition by key
        KeyedStream<ReducerExamplesItem, Integer> keyed = mapped.keyBy(x -> x.key);
        return keyed;
    }

    public DataStream<ReducerExamplesItem> generateInput(StreamExecutionEnvironment env, String methodName)
            throws NoSuchMethodException {

        // Step 1: set up input generator
        // Note: All these lines can be circumvented if one knows exactly which generator they want.
        // Then, they can just initialize it using the Step 2 code below.
        InputGenerator<DataStream<ReducerExamplesItem>> inputGen =
                new InputGenerator(env);
        Class[] cArg = new Class[1];
        cArg[0] = DataStream.class;
        Method testMethod = getClass().getMethod(methodName, DataStream.class, Boolean.class);
        Parameter parameter = testMethod.getParameters()[0];

        // Step 2: make input stream using generator
        Generator<DataStream<ReducerExamplesItem>> generator =
                (Generator<DataStream<ReducerExamplesItem>>) inputGen.parameterGenerator(parameter);
        DataStream<ReducerExamplesItem> stream = inputGen.generate(generator);

        return stream;
    }

//    /* =============== TEMPLATE FOR ALL TESTS =============== */
//
//    public <AccType, OutType> void mapReduceTest(
//            AggregateFunction<ReducerExamplesItem, AccType, OutType> testReducer,
//            Dependence<OutType> dependence
//            ) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<ReducerExamplesItem> testInput = generateInput(env, "identityMapper");
//
//        SingleOutputStreamOperator<OutType> seqOutput =
//                identityMapper(testInput, true)
//                .window(TumblingEventTimeWindows.of(Time.milliseconds(100)))
//                .aggregate(testReducer);
//        SingleOutputStreamOperator<OutType> parOutput =
//                identityMapper(testInput, false)
//                .window(TumblingEventTimeWindows.of(Time.milliseconds(100)))
//                .aggregate(testReducer);
//
//        StreamEquivalenceMatcher matcher =
//                StreamEquivalenceMatcher.createMatcher(seqOutput, parOutput, dependence);
//
//        env.execute();
//        matcher.assertStreamsAreEquivalent();
//    }
//
//    /* =============== TESTS =============== */
//
//    // 1A. SingleItem on bad input
//    @Test(expected = Exception.class)
//    public void testSingleItemIncorrect() throws Exception {
//        mapReduceTest(new SingleItemGroupReducer(), new FullDependence<Integer>());
//    }
//
//    // 1B. SingleItem on good input
//    @Test
//    public void testSingleItemCorrect() throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<ReducerExamplesItem> input = generateInput(env, "identityMapper");
//
//        KeyedStream<ReducerExamplesItem, Integer> seqOutput = identityMapper(input, true);
//        KeyedStream<ReducerExamplesItem, Integer> parOutput = identityMapper(input, false);
//
//        StreamEquivalenceMatcher matcher =
//                StreamEquivalenceMatcher.createMatcher(seqOutput, parOutput, new EmptyDependence<>());
//
//        env.execute();
//
//        matcher.assertStreamsAreEquivalent();
//    }
}
