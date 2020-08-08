package edu.upenn.diffstream.remote;

import edu.upenn.diffstream.Dependence;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RemoteMatcherFactory implements RemoteMatcherRepository {

    public static final String REMOTE_HOSTNAME = "localhost";
    public static final String REMOTE_MATCHER_REPOSITORY = "RemoteMatcherRepository";

    public static RemoteMatcherFactory instance = null;

    private final ConcurrentMap<Long, RemoteStreamEquivalenceMatcher<?>> matcherPool = new ConcurrentHashMap<>();

    private RemoteMatcherFactory() {

    }

    public static void init() throws RemoteException {
        if (instance == null) {
            instance = new RemoteMatcherFactory();
            RemoteMatcherRepository remoteInstanceStub =
                    (RemoteMatcherRepository) UnicastRemoteObject.exportObject(instance, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind(REMOTE_MATCHER_REPOSITORY, remoteInstanceStub);
        }
    }

    public static RemoteMatcherFactory getInstance() {
        return instance;
    }

    public static void destroy() throws RemoteException {
        if (instance != null) {
            UnicastRemoteObject.unexportObject(instance, true);
            Registry registry = LocateRegistry.getRegistry();
            try {
                registry.unbind(REMOTE_MATCHER_REPOSITORY);
            } catch (NotBoundException ignored) {

            }
            instance = null;
        }
    }

    public <IN extends Serializable> RemoteStreamEquivalenceMatcher<IN> createMatcher(Dependence<IN> dependence,
                                                                                      boolean matcherLogItems) throws RemoteException {
        RemoteStreamEquivalenceMatcher<IN> matcher = new RemoteStreamEquivalenceMatcher<>(dependence, matcherLogItems);
        matcherPool.put(matcher.getId(), matcher);
        UnicastRemoteObject.exportObject(matcher, 0);
        return matcher;
    }

    public <IN extends Serializable> RemoteStreamEquivalenceMatcher<IN> createMatcher(DataStream<IN> leftStream,
                                                                                      DataStream<IN> rightStream,
                                                                                      Dependence<IN> dependence) throws RemoteException {
        RemoteStreamEquivalenceMatcher<IN> matcher = createMatcher(dependence, false);
        leftStream.addSink(matcher.getSinkLeft()).setParallelism(1);
        rightStream.addSink(matcher.getSinkRight()).setParallelism(1);
        return matcher;
    }

    public <IN extends Serializable> RemoteStreamEquivalenceMatcher<IN> createMatcher(DataStream<IN> leftStream,
                                                                                      DataStream<IN> rightStream,
                                                                                      Dependence<IN> dependence,
                                                                                      boolean matcherLogItems) throws RemoteException {
        RemoteStreamEquivalenceMatcher<IN> matcher = createMatcher(dependence, matcherLogItems);
        leftStream.addSink(matcher.getSinkLeft()).setParallelism(1);
        rightStream.addSink(matcher.getSinkRight()).setParallelism(1);
        return matcher;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <IN extends Serializable> RemoteMatcher<IN> getMatcherById(long matcherId) {
        return (RemoteMatcher<IN>) matcherPool.get(matcherId);
    }

    public void destroyMatcher(long matcherId) {
        RemoteStreamEquivalenceMatcher<?> matcher = matcherPool.remove(matcherId);
        try {
            UnicastRemoteObject.unexportObject(matcher, true);
        } catch (NoSuchObjectException ignored) {

        }
    }
}