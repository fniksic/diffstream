package edu.upenn.streamstesting.examples.flinktraining;

import edu.upenn.streamstesting.Matcher;
import edu.upenn.streamstesting.MatcherSink;
import edu.upenn.streamstesting.SinkBasedMatcher;
import edu.upenn.streamstesting.utils.ConstantKeySelector;
import edu.upenn.streamstesting.FullDependence;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;


import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.solutions.datastream_java.windows.HourlyTipsSolution;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple3;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.apache.flink.streaming.api.windowing.time.Time;
import com.ververica.flinktraining.solutions.datastream_java.windows.HourlyTipsSolution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertFalse;

public class WindowsTest {

	private static final Logger LOG = LoggerFactory.getLogger(WindowsTest.class);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
        new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
		.setNumberSlotsPerTaskManager(2)
		.setNumberTaskManagers(1)
		.build());
    
    public DataStream<Tuple3<Long, Long, Float>>
	correctImplementation(DataStream<TaxiFare> fares) {
	
	// compute tips per hour for each driver
	DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
	    .keyBy((TaxiFare fare) -> fare.driverId)
	    .timeWindow(Time.hours(1))
	    .process(new HourlyTipsSolution.AddTips());

	DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
	    .timeWindowAll(Time.hours(1))
	    .maxBy(2);

	return hourlyMax;
    }
    
    public DataStream<Tuple3<Long, Long, Float>>
	wrongImplementation(DataStream<TaxiFare> fares) {
	
	// compute tips per hour for each driver
	DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
	    .keyBy((TaxiFare fare) -> fare.driverId)
	    .timeWindow(Time.hours(1))
	    .process(new HourlyTipsSolution.AddTips());

	// You should explore how this alternative behaves. In what ways is the same as,
	// and different from, the solution above (using a timeWindowAll)?
	DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
	    .keyBy(0)
	    .maxBy(2);

	return hourlyMax;
    }
    
    //@Ignore
    @Test
    public void testSumTipFiniteInput() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//		MatcherSink sink = new MatcherSink();
	
		TaxiFare oneFor1In1 = testFare(1, t(0), 1.0F);
		TaxiFare fiveFor1In1 = testFare(1, t(15), 5.0F);
		TaxiFare tenFor1In2 = testFare(1, t(90), 10.0F);
		TaxiFare twentyFor2In2 = testFare(2, t(90), 20.0F);


		TestFareSource source = new TestFareSource(
			oneFor1In1,
			fiveFor1In1,
			tenFor1In2,
			twentyFor2In2);

		DataStream<TaxiFare> input = env.addSource(source);

		DataStream<Tuple3<Long, Long, Float>> correctOutput =
			correctImplementation(input);

		DataStream<Tuple3<Long, Long, Float>> wrongOutput =
			wrongImplementation(input);

		SinkBasedMatcher<Tuple3<Long, Long, Float>> matcher = SinkBasedMatcher.createMatcher(new FullDependence<>());
		correctOutput.addSink(matcher.getSinkLeft()).setParallelism(1);
		wrongOutput.addSink(matcher.getSinkRight()).setParallelism(1);

		env.execute();

		assertFalse("The two implementations should be equivalent", matcher.streamsAreEquivalent());

    }

    @Ignore
    public void testSumTipFileInput() throws Exception {
       
	// This is for getting data from a file 
	
	// // read parameters
	// ParameterTool params = ParameterTool.fromArgs(args);
	// final String input = params.get("input", ExerciseBase.pathToFareData);
	
	// final int maxEventDelay = 60;       // events are out of order by max 60 seconds
	// final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second
	
	// // set up streaming execution environment
	// StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	// env.setParallelism(ExerciseBase.parallelism);
	
	// // start the data generator
	// DataStream<TaxiFare> fares =
	// 	    env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));
    }
    
    // Util
    public long t(int n) {
	    return new DateTime(2000, 1, 1, 0, 0, DateTimeZone.UTC).plusMinutes(n).getMillis();
    }

    public TaxiFare testFare(long driverId, long startTime, float tip) {
	    return new TaxiFare(0, 0, driverId, new DateTime(startTime), "", tip, 0F, 0F);
    }

    private static class TestFareSource implements SourceFunction<TaxiFare> {
		private volatile boolean running = true;
		protected Object[] testStream;

		public TestFareSource(Object ... eventsOrWatermarks) {
			this.testStream = eventsOrWatermarks;
		}

		@Override
		public void run(SourceContext ctx) throws Exception {
			for (int i = 0; (i < testStream.length) && running; i++) {
				if (testStream[i] instanceof TaxiFare) {
					TaxiFare fare = (TaxiFare) testStream[i];
					ctx.collectWithTimestamp(fare, fare.getEventTime());
				} else if (testStream[i] instanceof String) {
					String s = (String) testStream[i];
					ctx.collectWithTimestamp(s, 0);
				} else if (testStream[i] instanceof Long) {
					Long ts = (Long) testStream[i];
					ctx.emitWatermark(new Watermark(ts));
				} else {
					throw new RuntimeException(testStream[i].toString());
				}
			}
			// test sources are finite, so they have a Long.MAX_VALUE watermark when they finishes
		}

		@Override
		public void cancel() {
			running = false;
		}

	}

}
