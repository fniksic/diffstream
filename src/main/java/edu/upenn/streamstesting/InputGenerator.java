package edu.upenn.streamstesting;

import java.util.Random;
import java.util.List;

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
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

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

		// TODO: Catch initialization error.
		// JUnitQuickcheck main = new JUnitQuickcheck(this.getClass());

		// TODO: This is protected. Either change it or find a way around it.
		// List<FrameworkMethod> methods = main.computeTestMethods();
		// Statement statement = main.methodBlock(methods.get(0));

		// Now that I have the statement, I can probably find its arguments.
		//
		// Following is a private method of Property statement
		//
		// private PropertyParameterContext parameterContextFor(
		//    Parameter parameter,
		//    Map<String, Type> typeVariables) {
		
		//     return new PropertyParameterContext(
		//         new ParameterTypeContext(
		//             parameter.getName(),
		//             parameter.getAnnotatedType(),
		//             declarerName(parameter),
		//             typeVariables)
		//             .allowMixedTypes(true)
		//     ).annotate(parameter);
		// }

		// Then I can use the sampler.decideGenerator for each argument.
		//
		// public Generator<?> decideGenerator(
		//          GeneratorRepository repository,
		//          ParameterTypeContext p)
		//
		// where the ParameterTypeContext can be gotten from PropertyParameterContext
		//
		// paramContext.typeContext()
		
		// TODO: In order to parametrize this, I could follow the tree from JUnitQuickcheck.java

		
		while (isRunning) {
			// Integer curr = gen.generate(prng);
			Integer curr = gen.generate(rand, status);
			sourceContext.collect(curr);
		}
	}

	@Test
	public void testMethod(DataStream<Integer> input) {
		return;
	}
	
	@Override
	public void cancel() {
		isRunning = false;
	}
}
