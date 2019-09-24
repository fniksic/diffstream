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
public class InputGenerator<T> implements SourceFunction<Integer> {

	// KK: From what I understand isRunning must be volatile
	//     because the `run` function runs indefinitely until
	//     cancel is called by another thread.
	private volatile boolean isRunning = true;

	// Fields needed for generation
	Random randGen;
	SourceOfRandomness rand;
	GeometricDistribution geo;
	SimpleGenerationStatus status;
	ParameterSampler sampler;
	GeneratorRepository genRepo;

	
	public InputGenerator() {
		// Instantiate source of randomness and generation status to call the generator
	        randGen = new Random();
	        rand = new SourceOfRandomness(randGen);
	        geo = new GeometricDistribution();
	        status = new SimpleGenerationStatus(geo, rand, 0);

		// It seems that I have to initialize and call a
		// parameter sampler
	        sampler = new TupleParameterSampler(100); // 100 is the number of trials

		// Initializing a generator repository so that I can
		// call the sampler to decide the generator on a
		// parameter.
	        genRepo = new GeneratorRepository(rand);
	}

	@Override
	public void run(SourceFunction.SourceContext<Integer> sourceContext) {
		
		// Instantiate a new integer generator
		// TODO: Make this all parametrizable
		IntegerGenerator gen = new IntegerGenerator();

		List<Method> testMethods = getClassTestMethods(getClass()); 

		Parameter parameter = testMethods.get(0).getParameters()[0];

		Generator<?> generator =
			parameterGenerator(parameter);
		generator.generate(rand, status);

		// TODO: Now that I have both a generator for the type
		// of the parameter, as well as its type using
		// reflection. I can instantiate a source that takes
		// as a parameter the input collection as an array,
		// and I can then use it to test some sinks.

		// TODO: I should probably make a generator for
		// Datastream, that calls the generator for lists and
		// then just calls from collection.
		
		while (isRunning) {
			// Integer curr = gen.generate(prng);
			Integer curr = gen.generate(rand, status);
			sourceContext.collect(curr);
		}
	}

	public List<Method> getClassTestMethods(Class<?> clazz) {
		
		List<Method> testMethods = new ArrayList<>();
		
		for (Method method : clazz.getMethods()) {
			if (method.getAnnotation(Test.class) != null) {
				testMethods.add(method);
			}
		}
		
		return testMethods;
	}

	public Generator<?> parameterGenerator(Parameter parameter) {
		ParameterTypeContext paramTypeContext =
			new ParameterTypeContext(parameter.getName(),
						 parameter.getAnnotatedType(),
						 "", // I am not sure if passing an empty name can be a problem
						 emptyMap()).allowMixedTypes(true);
		
		Generator<?> generator =
			sampler.decideGenerator(genRepo, paramTypeContext);
		
		return generator;
	}

	public T generate(Generator<T> generator) {
		return generator.generate(rand, status);
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
