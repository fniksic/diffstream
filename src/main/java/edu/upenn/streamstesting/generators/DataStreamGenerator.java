package edu.upenn.streamstesting.generators;

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Stream;
import java.util.Arrays;

import com.pholser.junit.quickcheck.generator.*;
import com.pholser.junit.quickcheck.internal.Lists;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import com.pholser.junit.quickcheck.generator.java.util.ArrayListGenerator;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.GenerationStatus.Key;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;


/**
 * Note: In order for the generator to be discovered by the framework,
 * we need to add in in the resources/META-INF/services folder, in the
 * Generator file.
 */
public class DataStreamGenerator extends ComponentizedGenerator<DataStream>{

	
        public DataStreamGenerator() {
		super(DataStream.class);
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
		int size = status.size();

		Generator<?> generator = componentGenerators().get(0);
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

	@Override public int numberOfNeededComponents() {
            return 1;
        }

}
