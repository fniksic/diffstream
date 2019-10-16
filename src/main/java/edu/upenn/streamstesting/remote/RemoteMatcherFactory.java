package edu.upenn.streamstesting.remote;

import edu.upenn.streamstesting.Dependence;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
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
    public static RemoteMatcherRepository remoteInstanceStub = null;

    private final ConcurrentMap<Long, RemoteStreamEquivalenceMatcher<?>> matcherPool = new ConcurrentHashMap<>();

    private RemoteMatcherFactory() {

    }

    public static void init() throws RemoteException {
        if (instance == null) {
            instance = new RemoteMatcherFactory();
            remoteInstanceStub = (RemoteMatcherRepository) UnicastRemoteObject.exportObject(instance, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind(REMOTE_MATCHER_REPOSITORY, remoteInstanceStub);
        }
    }

    public static RemoteMatcherFactory getInstance() {
        return instance;
    }

    public static void destroy() throws RemoteException {
        if (instance != null) {
            UnicastRemoteObject.unexportObject(remoteInstanceStub, true);
            Registry registry = LocateRegistry.getRegistry();
            try {
                registry.unbind(REMOTE_MATCHER_REPOSITORY);
            } catch (NotBoundException e) {

            }
            instance = null;
            remoteInstanceStub = null;
        }
    }

    public <IN extends Serializable> RemoteStreamEquivalenceMatcher<IN> createMatcher(Dependence<IN> dependence) {
        RemoteStreamEquivalenceMatcher<IN> matcher = new RemoteStreamEquivalenceMatcher<>(dependence);
        matcherPool.put(matcher.getId(), matcher);
        return matcher;
    }

    public <IN extends Serializable> RemoteStreamEquivalenceMatcher<IN> createMatcher(DataStream<IN> leftStream,
                                                                                      DataStream<IN> rightStream,
                                                                                      Dependence<IN> dependence) {
        RemoteStreamEquivalenceMatcher<IN> matcher = createMatcher(dependence);
        leftStream.addSink(matcher.getSinkLeft()).setParallelism(1);
        rightStream.addSink(matcher.getSinkRight()).setParallelism(1);
        return matcher;
    }

    @Override
    public <IN extends Serializable> RemoteMatcher<IN> getMatcherById(long matcherId) throws RemoteException {
        return (RemoteMatcher<IN>) matcherPool.get(matcherId);
    }

    public void destroyMatcher(long matcherId) {
        matcherPool.remove(matcherId);
    }
}
