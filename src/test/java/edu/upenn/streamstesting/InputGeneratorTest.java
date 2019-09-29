package edu.upenn.streamstesting;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Ignore;
import org.junit.Test;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import com.pholser.junit.quickcheck.generator.Generator;

import java.util.ArrayList;

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

    // @Test
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

    @Test
    public void testArrayListGenerator1() throws Exception {

	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	    InputGenerator<ArrayList<Integer>> inputGen = new InputGenerator(env);

	    Class[] cArg = new Class[1];
	    cArg[0] = ArrayList.class;

	    Method testMethod = getClass().getMethod("testArrayList1", cArg);

	    Parameter parameter = testMethod.getParameters()[0];
	    
	    Generator<ArrayList<Integer>> generator =
		    (Generator<ArrayList<Integer>>) inputGen.parameterGenerator(parameter);
	    ArrayList<Integer> item = inputGen.generate(generator);
	    ArrayList<Integer> output = testArrayList1(item);
	    System.out.println(output);
    }


    public ArrayList<Integer> testArrayList1(ArrayList<Integer> param) {
	    return param;
    }
	
    @Test
    public void testDataStreamGenerator1() throws Exception {

	    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	    
	    InputGenerator<DataStream<Integer>> inputGen = new InputGenerator(env);

	    Class[] cArg = new Class[1];
	    cArg[0] = DataStream.class;

	    Method testMethod = getClass().getMethod("testDataStream1", cArg);

	    Parameter parameter = testMethod.getParameters()[0];

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
