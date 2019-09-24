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

        InputGenerator gen = new InputGenerator();
	
	DataStream<Integer> ds = env.addSource(gen);

	ds.print();
	
	env.execute();
    }

    @Ignore
    public void testIntegerGenerator1() throws Exception {

	    InputGenerator<Integer> inputGen = new InputGenerator();
	    
	    Class[] cArg = new Class[1];
	    cArg[0] = Integer.class;

	    Method testMethod = getClass().getMethod("testInteger1", cArg);

	    Parameter parameter = testMethod.getParameters()[0];
	    
	    Generator<Integer> generator =
		    (Generator<Integer>) inputGen.parameterGenerator(parameter);
	    Integer item = inputGen.generate(generator);
	    Integer output = testInteger1(item);
	    // TODO: Maybe print that
    }


    public Integer testInteger1(Integer param) {
	    return param;
    }
}
