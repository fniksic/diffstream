package edu.upenn.streamstesting;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Ignore;
import org.junit.Test;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import com.pholser.junit.quickcheck.generator.Generator;

import edu.upenn.streamstesting.InputGenerator;

public class InputGeneratorTest {

    @Ignore
    public void testInputGenerator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        InputGenerator gen = new InputGenerator(env);
	
	DataStream<Integer> ds = env.addSource(gen);

	ds.print();
	
	env.execute();
    }

    @Test
    public void testIntegerGenerator1() throws Exception {

	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	    
	    InputGenerator<Integer> inputGen = new InputGenerator(env);

	    Class[] cArg = new Class[1];
	    cArg[0] = Integer.class;

	    Method testMethod = getClass().getMethod("testInteger1", cArg);

	    Parameter parameter = testMethod.getParameters()[0];
	    
	    Generator<Integer> generator =
		    (Generator<Integer>) inputGen.parameterGenerator(parameter);
	    Integer item = inputGen.generate(generator);
	    Integer output = testInteger1(item);
	    System.out.println("Random Integer: " + output);
    }


    public Integer testInteger1(Integer param) {
	    return param;
    }

    @Ignore
    public void testDataStreamGenerator1() throws Exception {

	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	    
	    InputGenerator<DataStream<Integer>> inputGen = new InputGenerator(env);

	    Class[] cArg = new Class[1];
	    cArg[0] = DataStream.class;

	    Method testMethod = getClass().getMethod("testDataStream1", cArg);

	    Parameter parameter = testMethod.getParameters()[0];

	    // The problem seems to be that the newInstance() calls
	    // fails on the DataStream class. I suspect that this is
	    // because DataStream can not be constructed with zero
	    // arguments.
	    //
	    // TODO: Investigate
	    Generator<DataStream<Integer>> generator =
		    (Generator<DataStream<Integer>>) inputGen.parameterGenerator(parameter);
	    DataStream<Integer> stream = inputGen.generate(generator);
	    testDataStream1(stream);
	    env.execute();
    }


    public void testDataStream1(DataStream<Integer> stream) throws Exception {
	stream.print();
    }
}
