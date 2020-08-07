package edu.upenn.diffstream.generators;

import com.pholser.junit.quickcheck.generator.ComponentizedGenerator;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.GenerationStatus.Key;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.internal.Ranges;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;
import java.util.stream.Stream;


/**
 * Note: In order for the generator to be discovered by the framework,
 * we need to add in in the resources/META-INF/services folder, in the
 * Generator file.
 */
public class DataStreamGenerator extends ComponentizedGenerator<DataStream>{

	private Size sizeRange;
	private WithTimestamps withTimestamps;

	public DataStreamGenerator() {
		super(DataStream.class);
    }

	private static Object extractValue(Object x) {
		Tuple2<Integer, ?> tuple = (Tuple2<Integer, Object>) x;
		return tuple.f1;
	}

	public void configure(Size size) {
		this.sizeRange = size;
		Ranges.checkRange(Ranges.Type.INTEGRAL, size.min(), size.max());
	}

	public void configure(WithTimestamps timestamps) {
		this.withTimestamps = timestamps;
	}

	private int size(SourceOfRandomness random, GenerationStatus status) {
		return this.sizeRange != null ? random.nextInt(this.sizeRange.min(), this.sizeRange.max()) : status.size();
	}

	@Override public DataStream<?> generate(SourceOfRandomness random,
					GenerationStatus status) {
		StreamExecutionEnvironment env =
			status
			.valueOf(new Key<StreamExecutionEnvironment>("flink-env",
								     StreamExecutionEnvironment.class))
			.get();

		// TODO: Allow configuring the DataStream Generator
		// (e.g. it sizerange) similarly to the
		// CollectionGenerator
		int size = this.size(random, status);

		Generator<?> generator = componentGenerators().get(0);

		// TODO: Find a way to get rid of duplication
		if(withTimestamps != null) {
			Counter counter = new Counter();

			Stream<?> itemStream =
					Stream.generate(() -> new Tuple2<>(counter.next(), generator.generate(random, status)))
							.sequential();

			ArrayList items = new ArrayList();
			itemStream.limit(size).forEach(items::add);

			DataStream<?> output = env.fromCollection(items)
					.assignTimestampsAndWatermarks(new TimestampWatermarkAssigner())
					.map(DataStreamGenerator::extractValue);
			return (output);
		} else {
			Stream<?> itemStream =
					Stream.generate(() -> generator.generate(random, status))
							.sequential();
			// TODO: Allow configuring distinctness or some other
			// configuration parameter of the generator that might
			// be needed for better DataStreamGeneration
			// if (distinct)
			// 	itemStream = itemStream.distinct();

			ArrayList items = new ArrayList();
			itemStream.limit(size).forEach(items::add);
			return (env.fromCollection(items));
		}
	}

	@Override public int numberOfNeededComponents() {
            return 1;
        }

	public class Counter {
		private Integer counter;

		public Counter() { this.counter = 0; }

		public Integer next() {
			this.counter += 1;
			return this.counter;
		}
	}

    public class TimestampWatermarkAssigner<MyElement> implements AssignerWithPunctuatedWatermarks<Tuple2<Integer, MyElement>> {
		public long extractTimestamp(Tuple2<Integer, MyElement> element, long previousElementTimestamp) {
			return element.f0;
		}

		// TODO: Configure this
		public Watermark checkAndGetNextWatermark(Tuple2<Integer, MyElement> lastElement, long extractedTimestamp) {
			return (extractedTimestamp % 100 == 0) ? new Watermark(extractedTimestamp) : null;
		}
	}

}
