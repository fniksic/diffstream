package edu.upenn.streamstesting;

import java.util.List;

import com.pholser.junit.quickcheck.generator.*;
import com.pholser.junit.quickcheck.internal.Lists;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import com.pholser.junit.quickcheck.generator.java.util.ArrayListGenerator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;


public class DataStreamGenerator extends ComponentizedGenerator<DataStream>{

	private StreamExecutionEnvironment env;
	private ArrayListGenerator superGen;
	
        public DataStreamGenerator(StreamExecutionEnvironment env) {
		super(DataStream.class);
		
		this.env = env;
		this.superGen = new ArrayListGenerator();
	}
	
        // @SuppressWarnings("unchecked")
        @Override public DataStream generate(SourceOfRandomness random,
				    GenerationStatus status) {

	        return (env.fromCollection(superGen.generate(random, status)));
	}
}
