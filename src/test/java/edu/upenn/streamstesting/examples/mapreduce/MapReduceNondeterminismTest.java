package edu.upenn.streamstesting.examples.mapreduce;

import com.pholser.junit.quickcheck.generator.Generator;
import edu.upenn.streamstesting.*;
import edu.upenn.streamstesting.examples.flinktraining.WindowsTest;
import edu.upenn.streamstesting.examples.mapreduce.reducers.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

public class MapReduceNondeterminismTest {

    /* =============== BOILERPLATE TO HELP RUN THE TESTS =============== */

    private static final Logger LOG = LoggerFactory.getLogger(WindowsTest.class);

    private static final int PARALLELISM = 2;
    private static final boolean DEBUG = true;

    private static final int TEST_ITERATIONS = 5;
    private static final int TEST_STREAM_FUEL = 100;

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

/*
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
*/

    public <T> DataStream<T> printMapper(String info, DataStream<T> inStream) {
        return inStream.map(new MapFunction<T, T>() {
            @Override
            public T map(T t) throws Exception {
                System.out.println(info + t);
                return t;
            }
        });
    }

    /* =============== TEMPLATE FOR ALL TESTS =============== */

    // This template requires that the reducer be implemented as an AggregateFunction
    public <AccType, OutType> void mapReduceTest(
            SourceFunction<ReducerExamplesItem> inputSource,
            AggregateFunction<ReducerExamplesItem, AccType, OutType> testReducer,
            Dependence<OutType> dependence
    ) throws Exception {
        for (int iteration = 0; iteration < TEST_ITERATIONS; iteration++) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<ReducerExamplesItem> testInput =
                    env.addSource(inputSource)
                    .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReducerExamplesItem>() {
                        @Override
                        public long extractAscendingTimestamp(ReducerExamplesItem reducerExamplesItem) {
                            return reducerExamplesItem.timestamp.getTime();
                        }
                    });

            SingleOutputStreamOperator<OutType> seqOutput =
                    identityMapper(testInput, true)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                    .aggregate(testReducer);
            SingleOutputStreamOperator<OutType> parOutput =
                    identityMapper(testInput, false)
                    .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                    .aggregate(testReducer);

            if (DEBUG) {
                DataStream<OutType> printer1 = printMapper("SEQ OUTPUT: ", seqOutput);
                DataStream<OutType> printer2 = printMapper("PAR OUTPUT: ", parOutput);
            }

            StreamEquivalenceMatcher matcher =
                    StreamEquivalenceMatcher.createMatcher(seqOutput, parOutput, dependence);

            env.execute();
            matcher.assertStreamsAreEquivalent();
        }
    }

    /* =============== TESTS =============== */

    // 1A. SingleItem on bad input
    @Ignore
    @Test(expected = Exception.class)
    public void testSingleItemIncorrect() throws Exception {
        mapReduceTest(
                new BadDataSource(TEST_STREAM_FUEL),
                new SingleItemReducer(),
                new EmptyDependence<>()
        );
    }

    // 1B. SingleItem on good input
    @Ignore
    @Test
    public void testSingleItemCorrect() throws Exception {
        mapReduceTest(
                new GoodDataSource(TEST_STREAM_FUEL),
                new SingleItemReducer(),
                new EmptyDependence<>()
        );
    }

    // 2A. IndexValuePair on bad input
    @Ignore
    @Test(expected = Exception.class)
    public void testIndexValuePairIncorrect() throws Exception {
        mapReduceTest(
                new BadDataSource(TEST_STREAM_FUEL),
                new IndexValuePairReducer(),
                new EmptyDependence<>()
        );
    }

    // 2B. IndexValuePair on good input
    @Ignore
    @Test
    public void testIndexValuePairCorrect() throws Exception {
        mapReduceTest(
                new GoodDataSource(TEST_STREAM_FUEL),
                new IndexValuePairReducer(),
                new EmptyDependence<>()
        );
    }

    // 3A. MaxRow on bad input
    @Ignore
    @Test(expected = Exception.class)
    public void testMaxRowIncorrect() throws Exception {
        mapReduceTest(
                new BadDataSource(TEST_STREAM_FUEL),
                new MaxRowReducer(),
                new EmptyDependence<>()
        );
    }

    // 3B. MaxRow on good input
    @Ignore
    @Test
    public void testMaxRowCorrect() throws Exception {
        mapReduceTest(
                new GoodDataSource(TEST_STREAM_FUEL),
                new MaxRowReducer(),
                new EmptyDependence<>()
        );
    }

    // 4A. FirstN on bad input
    @Ignore
    @Test(expected = Exception.class)
    public void testFirstNIncorrect() throws Exception {
        mapReduceTest(
                new BadDataSource(101),
                new FirstNReducer(),
                new EmptyDependence<>()
        );
    }

    // 4B. FirstN on good input
    @Ignore
    @Test
    public void testFirstNCorrect() throws Exception {
        mapReduceTest(
                new BadDataSource(10),
                new FirstNReducer(),
                new EmptyDependence<>()
        );
    }

    // 5A. StrConcat with bad equality relation
    @Ignore
    @Test(expected = Exception.class)
    public void testStrConcatIncorrect() throws Exception {
        mapReduceTest(
                new BadDataSource(TEST_STREAM_FUEL),
                new StrConcatReducerV1(),
                new EmptyDependence<>()
        );
    }

    // 5B. StrConcat with overrided equality
    // TODO
//    @Test
//    public void testFirstNCorrect() throws Exception {
//        mapReduceTest(
//                new GoodDataSource(10),
//                new FirstNReducer(),
//                new EmptyDependence<>()
//        );
//    }


}
