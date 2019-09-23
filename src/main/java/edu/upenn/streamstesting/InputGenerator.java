package edu.upenn.streamstesting;

import java.util.ArrayList;
import java.util.Random;
import java.util.List;
import java.util.Map;
import static java.util.Collections.*;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

// Input generation
import com.pholser.junit.quickcheck.generator.java.lang.IntegerGenerator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import com.pholser.junit.quickcheck.internal.generator.SimpleGenerationStatus;
import com.pholser.junit.quickcheck.internal.GeometricDistribution;

// Sampling parameters
import com.pholser.junit.quickcheck.internal.ParameterSampler;
// import com.pholser.junit.quickcheck.internal.sampling.ExhaustiveParameterSampler;
import com.pholser.junit.quickcheck.internal.sampling.TupleParameterSampler;

// Rest needed for input generation
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.internal.generator.GeneratorRepository;
import com.pholser.junit.quickcheck.internal.ParameterTypeContext;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import com.pholser.junit.quickcheck.generator.Generator;


// import org.quicktheories.generators.IntegersDSL;
// import org.quicktheories.core.Gen;
// import org.quicktheories.core.RandomnessSource;
// import org.quicktheories.core.XOrShiftPRNG;

// KK: We should probably extend this to be a checkpointable source
//
// KK: This might have to be parametrized by some type??
public class InputGenerator implements SourceFunction<Integer> {

	// KK: From what I understand isRunning must be volatile
	//     because the `run` function runs indefinitely until
	//     cancel is called by another thread.
	private volatile boolean isRunning = true;
	
	public InputGenerator() { }

	@Override
	public void run(SourceFunction.SourceContext<Integer> sourceContext) {
		
		// Instantiate a new integer generator
		// TODO: Make this all parametrizable
		IntegerGenerator gen = new IntegerGenerator();

		// Instantiate source of randomness and generation status to call the generator
		Random randGen = new Random();
		SourceOfRandomness rand = new SourceOfRandomness(randGen);
		GeometricDistribution geo = new GeometricDistribution();
	        SimpleGenerationStatus status = new SimpleGenerationStatus(geo, rand, 0);

		// Quicktheories
		// IntegersDSL intGen = new IntegersDSL();
		// Gen<Integer> gen = intGen.all();
		// // TODO: Have a random see maybe?
		// XOrShiftPRNG prng = new XOrShiftPRGN(1);

		// Notes:
		//
		// - It seems that I have to initialize and call a parameter sampler
		ParameterSampler sampler = new TupleParameterSampler(100); // 100 is the number of trials

		// Initializing a generator repository so that I can
		// call the sampler to decide the generator on a
		// parameter.
		GeneratorRepository genRepo = new GeneratorRepository(rand);

		// Maybe instead of doing it for a simple argument I
		// need to find a way to do it for the whole property?
		// The property is the statement that we test, or the
		// flink program.
		//
		// PropertyStatement(
		// 		  FrameworkMethod method,
		// 		  TestClass testClass,
		// 		  GeneratorRepository repo,
		// 		  GeometricDistribution distro,
		// 		  Logger logger)

		// I am temporarily using that to extract what I
		// really need to use for the generation.

		List<Method> testMethods = new ArrayList<>();
		
		for (Method method : getClass().getMethods()) {
			if (method.getAnnotation(Test.class) != null) {
				testMethods.add(method);
			}
		}

		Parameter parameter = testMethods.get(0).getParameters()[0];

		ParameterTypeContext paramTypeContext =
			new ParameterTypeContext(parameter.getName(),
						 parameter.getAnnotatedType(),
						 "", // I am not sure if passing an empty name can be a problem
						 emptyMap()).allowMixedTypes(true);

		// 	Then I can use the sampler.decideGenerator for each argument.
		Generator<?> generator =
			sampler.decideGenerator(genRepo, paramTypeContext);			
		
		generator.generate(rand, status);

		// TODO: Now that I have both a generator for the type
		// of the parameter, as well as its type using
		// reflection. I can instantiate a source that takes
		// as a parameter the input collection as an array,
		// and I can then use it to test some sinks.


		// The commented block here is probably useless
		//
		// TODO: Catch initialization error.
		// try {
		//         TestClass thisClass = new TestClass(getClass());
		
		// 	JUnitQuickcheck main = new JUnitQuickcheck(getClass());
			
		// 	List<FrameworkMethod> testMethods = new ArrayList<>();
		// 	testMethods.addAll(thisClass.getAnnotatedMethods(Property.class));
			
		// 	PropertyStatement statement = main.methodBlock(testMethods.get(0));

		// 	import java.lang.reflect.Parameter;
		// 	        new ParameterTypeContext(
		// 	            parameter.getName(),
		// 	            parameter.getAnnotatedType(),
		// 	            declarerName(parameter),
		// 	            typeVariables)
		// 	            .allowMixedTypes(true)
			
		// 	Now that I have the statement, I can probably find its arguments.
			
		// 	Following is a private method of Property statement
			
		// 	private PropertyParameterContext parameterContextFor(
		// 	   Parameter parameter,
		// 	   Map<String, Type> typeVariables) {
			
		// 	    return new PropertyParameterContext(
		// 	        new ParameterTypeContext(
		// 	            parameter.getName(),
		// 	            parameter.getAnnotatedType(),
		// 	            declarerName(parameter),
		// 	            typeVariables)
		// 	            .allowMixedTypes(true)
		// 	    ).annotate(parameter);
		// 	}
						
		// } catch (Exception e) {
		// 	// This is here to catch the possible throw of
		// 	// an initialization error
			
		// 	// TODO: I have to do something in case this
		// 	// fails
		// 	return;
		// }
		
		while (isRunning) {
			// Integer curr = gen.generate(prng);
			Integer curr = gen.generate(rand, status);
			sourceContext.collect(curr);
		}
	}

	@Test
	public void testMethod(Integer input) {
		return;
	}

	
	@Override
	public void cancel() {
		isRunning = false;
	}
}
