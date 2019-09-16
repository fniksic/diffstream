package edu.upenn.streamstesting;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Test;

import edu.upenn.streamstesting.InputGenerator;

public class InputGeneratorTest {

    @Test
    public void testInputGenerator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        InputGenerator gen = new InputGenerator();
	
	DataStream<Integer> ds = env.addSource(gen);

	ds.print();
	
	env.execute();
    }
}
