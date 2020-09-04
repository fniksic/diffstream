package edu.upenn.diffstream;

import com.pholser.junit.quickcheck.generator.GenerationStatus.Key;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.generator.java.lang.IntegerGenerator;
import com.pholser.junit.quickcheck.internal.GeometricDistribution;
import com.pholser.junit.quickcheck.internal.ParameterSampler;
import com.pholser.junit.quickcheck.internal.ParameterTypeContext;
import com.pholser.junit.quickcheck.internal.generator.GeneratorRepository;
import com.pholser.junit.quickcheck.internal.generator.ServiceLoaderGeneratorSource;
import com.pholser.junit.quickcheck.internal.generator.SimpleGenerationStatus;
import com.pholser.junit.quickcheck.internal.sampling.TupleParameterSampler;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.util.Collections.emptyMap;

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

    // Needed to turn collection into datastream
    StreamExecutionEnvironment env;

    public InputGenerator(StreamExecutionEnvironment env) {

        // TODO: Maybe remove that if not useful
        this.env = env;

        // Instantiate source of randomness and generation status to call the generator
        randGen = new Random();
        rand = new SourceOfRandomness(randGen);
        geo = new GeometricDistribution();
        status = new StreamGenerationStatus(geo, rand, 0);

        // Save the environment in the status so that it can
        // be accessed by the DataStream generator
        status.setValue(new Key<>("flink-env", StreamExecutionEnvironment.class), env);

        // It seems that I have to initialize and call a
        // parameter sampler
        sampler = new TupleParameterSampler(100); // 100 is the number of trials

        // Initializing a generator repository so that I can
        // call the sampler to decide the generator on a
        // parameter.
        //
        // The service loader generator source finds all
        // defined generators (i.e. those defined by the
        // framework and those in the
        // resources/META-INF/services/...Generator file) and
        // loads them so that they can be used for deciding
        genRepo = new GeneratorRepository(rand).register(new ServiceLoaderGeneratorSource());
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

    @Override
    public void cancel() {
        isRunning = false;
    }

}
