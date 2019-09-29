package edu.upenn.streamstesting;

import java.util.List;

import com.pholser.junit.quickcheck.generator.*;
import com.pholser.junit.quickcheck.internal.Lists;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import com.pholser.junit.quickcheck.generator.java.util.ArrayListGenerator;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.GenerationStatus.Key;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.util.Arrays;

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
		// TODO: Make the generator really output a stream instead of just an element
		return (env.fromCollection(Arrays.asList(componentGenerators().get(0).generate(random, status))));
	}

	@Override public int numberOfNeededComponents() {
            return 1;
        }

}
