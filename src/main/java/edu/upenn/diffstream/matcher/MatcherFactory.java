package edu.upenn.diffstream.matcher;

import edu.upenn.diffstream.Dependence;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class MatcherFactory implements RemoteMatcherRepository {

    private static final Logger LOG = LoggerFactory.getLogger(MatcherFactory.class);

    /* We maintain a pool of matchers to deal with the serialization issues in Flink.
       Namely, sinks in Flink need to be serializable, so they cannot have an explicit, baked in
       reference to the corresponding matcher. Instead, a sink retrieves its corresponding
       matcher from the pool during initialization.
     */

    private static final AtomicLong matcherCount = new AtomicLong(0L);
    private static final ConcurrentMap<Long, StreamEquivalenceMatcher<?>> matcherPool = new ConcurrentHashMap<>();

    private static MatcherFactory remoteInstance = null;

    public static void initRemote() throws RemoteException {
        LOG.debug("Initializing remote matcher repository");
        if (remoteInstance == null) {
            remoteInstance = new MatcherFactory();
            RemoteMatcherRepository remoteInstanceStub =
                    (RemoteMatcherRepository) UnicastRemoteObject.exportObject(remoteInstance, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind(REMOTE_MATCHER_REPOSITORY, remoteInstanceStub);
        }
    }

    public static void destroyRemote() throws RemoteException {
        LOG.debug("Destroying remote matcher repository");
        if (remoteInstance != null) {
            UnicastRemoteObject.unexportObject(remoteInstance, true);
            Registry registry = LocateRegistry.getRegistry();
            try {
                registry.unbind(REMOTE_MATCHER_REPOSITORY);
            } catch (NotBoundException ignored) {

            }
            remoteInstance = null;
        }
    }

    private static <IN extends Serializable> StreamEquivalenceMatcher<IN> createMatcher(Dependence<IN> dependence,
                                                                                        boolean isRemote) {
        long id = matcherCount.incrementAndGet();
        StreamEquivalenceMatcher<IN> matcher = new StreamEquivalenceMatcher<>(id, dependence, isRemote);
        matcherPool.put(matcher.getId(), matcher);
        return matcher;
    }

    public static <IN extends Serializable> StreamEquivalenceMatcher<IN> createMatcher(DataStream<IN> out1,
                                                                                       DataStream<IN> out2,
                                                                                       Dependence<IN> dependence) {
        StreamEquivalenceMatcher<IN> matcher = createMatcher(dependence, false);
        out1.addSink(new StreamEquivalenceSink<>(matcher.getId(), true)).setParallelism(1);
        out2.addSink(new StreamEquivalenceSink<>(matcher.getId(), false)).setParallelism(1);
        LOG.debug("Created a matcher with id={}", matcher.getId());
        return matcher;
    }

    public static <IN extends Serializable> StreamEquivalenceMatcher<IN> createRemoteMatcher(DataStream<IN> out1,
                                                                                             DataStream<IN> out2,
                                                                                             Dependence<IN> dependence) throws RemoteException {
        StreamEquivalenceMatcher<IN> matcher = createMatcher(dependence, true);
        out1.addSink(new RemoteStreamEquivalenceSink<>(matcher.getId(), true)).setParallelism(1);
        out2.addSink(new RemoteStreamEquivalenceSink<>(matcher.getId(), false)).setParallelism(1);
        UnicastRemoteObject.exportObject(matcher, 0);
        LOG.debug("Created a remote matcher with id={}", matcher.getId());
        return matcher;
    }

    @SuppressWarnings("unchecked")
    public static <IN extends Serializable> StreamEquivalenceMatcher<IN> getMatcher(long matcherId) {
        return (StreamEquivalenceMatcher<IN>) matcherPool.get(matcherId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <IN extends Serializable> RemoteMatcher<IN> getRemoteMatcher(long matcherId) {
        return (RemoteMatcher<IN>) matcherPool.get(matcherId);
    }

    public static <IN extends Serializable> void destroyMatcher(StreamEquivalenceMatcher<IN> matcher) {
        LOG.debug("Destroying a matcher with id={}", matcher.getId());
        matcherPool.remove(matcher.getId());
        if (matcher.isRemote()) {
            try {
                UnicastRemoteObject.unexportObject(matcher, true);
            } catch (NoSuchObjectException ignored) {

            }
        }
    }

}
