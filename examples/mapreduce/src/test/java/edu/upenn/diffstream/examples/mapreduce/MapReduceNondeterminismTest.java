package edu.upenn.diffstream.examples.mapreduce;

/* DiffStream Imports */
import edu.upenn.diffstream.Dependence;
import edu.upenn.diffstream.EmptyDependence;
import edu.upenn.diffstream.FullDependence;
import edu.upenn.diffstream.matcher.MatcherFactory;
import edu.upenn.diffstream.matcher.StreamEquivalenceMatcher;
import edu.upenn.diffstream.examples.mapreduce.reducers.*;

/* Flink Imports */
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps
                                               .AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners
                                               .TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.MiniClusterWithClientResource;

/* Other Imports */
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

public class MapReduceNondeterminismTest {

    /* =============== BOILERPLATE TO HELP RUN THE TESTS =============== */

    private static final Logger LOG =
        LoggerFactory.getLogger(MapReduceNondeterminismTest.class);

    private static final int PARALLELISM = 2;
    private static final boolean DEBUG = false;

    private static final int TEST_ITERATIONS = 1;
    private static final int TEST_STREAM_FUEL = 3000;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .build());

    // We use a map function which performs no computation, but may shuffle
    // the order of events between partitions.
    // Then it partitions the (now nondeterministically ordered) stream by key.
    // This allows us to test if a particular reducer is correct on
    // nondeterministically ordered input data.
    public KeyedStream<ReducerExamplesItem, Integer> identityMapper(
        DataStream<ReducerExamplesItem> events, Boolean isSequential) {

        // Stage 1: identity map. If not sequential, this should break the
        // input order
        int parallelism = isSequential ? 1 : PARALLELISM;
        DataStream<ReducerExamplesItem> mapped = events
            .map(x -> x)
            .setParallelism(parallelism);

        // Stage 2: re-partition by key
        return mapped.keyBy(x -> x.key);
    }

    public <T> DataStream<T> printMapper(String info, DataStream<T> inStream) {
        return inStream.map(new MapFunction<>() {
            @Override
            public T map(T t) {
                System.out.println(info + t);
                return t;
            }
        });
    }

    /* ==================== TEMPLATE FOR ALL TESTS ==================== */

    // A basic template for tests in DiffStream, which also prints the result
    // that is detected (streams equivalent or not equivalent)
    public <OutType extends Serializable> void diffStreamTest(
        String testInfo,
        String expectingInfo,
        StreamExecutionEnvironment flinkEnv,
        DataStream<OutType> seqOutput,
        DataStream<OutType> parOutput,
        Dependence<OutType> dependence
    ) throws Exception {
        System.out.println("Running Test: " + testInfo);
        System.out.println("    Expecting: " + expectingInfo);

        if (DEBUG) {
            DataStream<OutType> printer1 =
                printMapper("        SEQ OUTPUT: ", seqOutput);
            DataStream<OutType> printer2 =
                printMapper("        PAR OUTPUT: ", parOutput);
        }

        try (var matcher = MatcherFactory.createMatcher(seqOutput, parOutput, dependence)) {
            flinkEnv.execute();
            matcher.assertStreamsAreEquivalent();
        }
        catch(Exception exc) {
            System.out.println(
                "    Result: streams differ"
            );
            throw exc;
        }
        System.out.println(
            "    Result: streams are equivalent"
        );
    }


    // More specific template for MapReduce, used throughout this file.
    // The template requires that the reducer be implemented as an
    // AggregateFunction.
    public <AccType, OutType extends Serializable> void mapReduceTest(
        String testInfo,
        String expectingInfo,
        SourceFunction<ReducerExamplesItem> inputSource,
        AggregateFunction<ReducerExamplesItem, AccType, OutType> reducer,
        Dependence<OutType> dependence
    ) throws Exception {

        for (int iteration = 0; iteration < TEST_ITERATIONS; iteration++) {
            StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<ReducerExamplesItem> testInput =
                env.addSource(inputSource).assignTimestampsAndWatermarks(
                    new AscendingTimestampExtractor<>() {
                        @Override
                        public long extractAscendingTimestamp(
                            ReducerExamplesItem reducerExamplesItem
                        ) {
                            return reducerExamplesItem.timestamp.getTime();
                        }
                    });

            SingleOutputStreamOperator<OutType> seqOutput =
                identityMapper(testInput, true)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(30)))
                .aggregate(reducer);
            SingleOutputStreamOperator<OutType> parOutput =
                identityMapper(testInput, false)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(30)))
                .aggregate(reducer);

            diffStreamTest(testInfo, expectingInfo,
                           env, seqOutput, parOutput, dependence);
        }
    }

    /* ========================= TESTS ========================= */

    // 1A. SingleItem on arbitrary input (BadDataSource)
    // This should throw StreamsNotEquivalentException due to nondeterminism.
    @Test(expected = Exception.class)
    public void testSingleItemIncorrect() throws Exception {
        mapReduceTest(
            "SingleItem (determinism on all inputs)",
            "differ",
            new BadDataSource(TEST_STREAM_FUEL),
            new SingleItemReducer(),
            new EmptyDependence<>()
        );
    }

    // 1B. SingleItem on well-formed input (GoodDataSource)
    // This should terminate normally (no nondeterminism).
    @Test
    public void testSingleItemCorrect() throws Exception {
        mapReduceTest(
            "SingleItem (determinism on well-formed input)",
            "equivalent",
            new GoodDataSource(TEST_STREAM_FUEL),
            new SingleItemReducer(),
            new EmptyDependence<>()
        );
    }

    // 2A. IndexValuePair on arbitrary input (BadDataSource)
    // This should throw StreamsNotEquivalentException due to nondeterminism.
    @Test(expected = Exception.class)
    public void testIndexValuePairIncorrect() throws Exception {
        mapReduceTest(
            "IndexValuePair (determinism on all inputs)",
            "differ",
            new BadDataSource(TEST_STREAM_FUEL),
            new IndexValuePairReducer(),
            new EmptyDependence<>()
        );
    }

    // 2B. IndexValuePair on well-formed input (GoodDataSource)
    // This should terminate normally (no nondeterminism).
    @Test
    public void testIndexValuePairCorrect() throws Exception {
        mapReduceTest(
            "IndexValuePair (determinism on well-formed input)",
            "equivalent",
            new GoodDataSource(TEST_STREAM_FUEL),
            new IndexValuePairReducer(),
            new EmptyDependence<>()
        );
    }

    // 3A. MaxRow on arbitrary input (BadDataSource)
    // This should throw StreamsNotEquivalentException due to nondeterminism.
    @Test(expected = Exception.class)
    public void testMaxRowIncorrect() throws Exception {
        mapReduceTest(
            "MaxRow (determinism on all inputs)",
            "differ",
            new BadDataSource(TEST_STREAM_FUEL),
            new MaxRowReducer(),
            new EmptyDependence<>()
        );
    }

    // 3B. MaxRow on well-formed input (GoodDataSource)
    // This should terminate normally (no nondeterminism).
    @Test
    public void testMaxRowCorrect() throws Exception {
        mapReduceTest(
            "MaxRow (determinism on well-formed input)",
            "equivalent",
            new GoodDataSource(TEST_STREAM_FUEL),
            new MaxRowReducer(),
            new EmptyDependence<>()
        );
    }

    // 4A. FirstN on arbitrary input
    // In this case "arbitrary input" means a larger number of input
    // items (100)
    // This should throw StreamsNotEquivalentException due to nondeterminism.
    @Test(expected = Exception.class)
    public void testFirstNIncorrect() throws Exception {
        mapReduceTest(
            "FirstN (determinism on all inputs)",
            "differ",
            new BadDataSource(100),
            new FirstNReducer(),
            new EmptyDependence<>()
        );
    }

    // 4B. FirstN on well-formed input (GoodDataSource)
    // In this case, "well-formed input" means a smaller number of input
    // items (10)
    // This should terminate normally (no nondeterminism).
    @Test
    public void testFirstNCorrect() throws Exception {
        mapReduceTest(
            "FirstN (determinism on well-formed input)",
            "equivalent",
            new BadDataSource(10),
            new FirstNReducer(),
            new EmptyDependence<>()
        );
    }

    // 5A. StrConcat, naive test
    // This should throw StreamsNotEquivalentException due to nondeterminism.
    @Test(expected = Exception.class)
    public void testStrConcatIncorrect() throws Exception {
        mapReduceTest(
            "StrConcat (determinism on all inputs)",
            "differ",
            new BadDataSource(TEST_STREAM_FUEL),
            new StrConcatReducer(),
            new EmptyDependence<>()
        );
    }

    // 5B. StrConcat, first corrected version: overrides equality

    // We first define a custom wrapper String class, which replaces default
    // String equality.
    // In this class, we check for equivalence by considering the string to be
    // a set of strings concatenated with "@".
    private static class StringWithSeparators implements Serializable {
        public String s;

        public StringWithSeparators(String s) {
            this.s = s;
        }

        @Override
        public boolean equals(Object other1) {
            if(other1 instanceof StringWithSeparators) {
                StringWithSeparators other = (StringWithSeparators) other1;
                if (DEBUG) {
                    System.out.println(
                        "        CHECKING EQUALITY: " + this + " and " + other
                    );
                }
                HashSet<String> thisSet = new HashSet<>(
                    Arrays.asList(this.s.split("@"))
                );
                HashSet<String> otherSet = new HashSet<>(
                    Arrays.asList(other.s.split("@"))
                );
                return thisSet.equals(otherSet);
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return s;
        }
    }

    // Next, we define a slightly-modified version of StrConcatReducer which
    // returns our custom StringWithSeparators class instead of a String.
    // This is identical to StrConcatReducer except for getResult.
    private static class StrConcatReducerOverridedEquality implements
            AggregateFunction<ReducerExamplesItem,
                              String,
                              StringWithSeparators> {
        public String createAccumulator() {
            return "";
        }
        public String add(ReducerExamplesItem newItem, String state) {
            return state + "@" + newItem.x;
        }
        public StringWithSeparators getResult(String state) {
            return new StringWithSeparators(state);
        }
        public String merge(String ignore1, String ignore2) {
            throw new RuntimeException("'merge' should not be called");
        }
    }

    // Then we run the test.
    // Because we have redefined equality to what we actually care about,
    // this should terminate normally (no nondeterminism).
    @Test
    public void testStrConcatCorrect() throws Exception {
        mapReduceTest(
            "StrConcat (nondeterminism)",
            "equivalent",
            new BadDataSource(20),
            new StrConcatReducerOverridedEquality(),
            new EmptyDependence<>()
        );
    }

    // 5C. StrConcat, re-implemented, naive test

    // Rather than concatenating the outputs as a string, since we are in
    // the streaming setting, we can rewrite the reducer to avoid
    // aggregation, and instead output each item individually as a separate
    // item of the output stream.

    // For this test and the next one only, we don't use mapReduceTest, but use
    // the following simplified version which is a basic implementation of
    // the above idea: it does not window and does not aggregate output at
    // all.
    public void mapReduceSimplifiedStrConcatTest(
        String testInfo,
        String expectingInfo,
        SourceFunction<ReducerExamplesItem> inputSource,
        Dependence<ReducerExamplesItem> dependence
    ) throws Exception {
        for (int iteration = 0; iteration < TEST_ITERATIONS; iteration++) {
            StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<ReducerExamplesItem> testInput =
                env.addSource(inputSource);

            KeyedStream<ReducerExamplesItem, Integer> seqOutput =
                identityMapper(testInput, true);
            KeyedStream<ReducerExamplesItem, Integer> parOutput =
                identityMapper(testInput, false);

            diffStreamTest(testInfo, expectingInfo,
                           env, seqOutput, parOutput, dependence);
        }
    }

    // Using the re-implemented streaming version, we now test for
    // determinism, requiring fully ordered output.
    // This should throw StreamsNotEquivalentException due to nondeterminism.
    @Test(expected = Exception.class)
    public void testStrConcatStreamOutputIncorrect() throws Exception {
        mapReduceSimplifiedStrConcatTest(
            "StrConcat, re-implemented streaming version" +
            " (determinism on all inputs)",
            "differ",
            new BadDataSource(20),
            new FullDependence<>()
        );
    }

    // 5D. StrConcat, re-implemented corrected version

    // Finally, using the same StrConcat re-implementation as in 5C,
    // we test allowing the output to be unordered.
    // Because we use the correct dependence relation, this should
    // terminate normally (no nondeterminism).
    @Test
    public void testStrConcatStreamOutputCorrect() throws Exception {
        mapReduceSimplifiedStrConcatTest(
            "StrConcat, re-implemented streaming version (nondeterminism)",
            "equivalent",
            new BadDataSource(20),
            new EmptyDependence<>()
        );
    }

}
