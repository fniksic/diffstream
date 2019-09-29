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
	
        public DataStreamGenerator() {
		super(DataStream.class);
		
		this.superGen = new ArrayListGenerator();
		this.env = StreamExecutionEnvironment.getExecutionEnvironment();
	}

	// Note: This has to be always called before generate. THe
	// problem is that I cannot give env in the constructor,
	// because the infastructure requires that a generator has 0
	// arguments.
	public void set_environment(StreamExecutionEnvironment env) {
		this.env = env;
	}
	
        // @SuppressWarnings("unchecked")
        @Override public DataStream<?> generate(SourceOfRandomness random,
				    GenerationStatus status) {

		// TODO: Check why the generate here fails :)
	        return (env.fromCollection(superGen.generate(random, status)));
	}

	@Override public int numberOfNeededComponents() {
            return 1;
        }

}
