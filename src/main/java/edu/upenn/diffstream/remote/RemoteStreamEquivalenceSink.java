package edu.upenn.diffstream.remote;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class RemoteStreamEquivalenceSink<IN extends Serializable> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = -1399560301030209146L;

    private long matcherId;

    /* Are we the left or the right sink? */
    private boolean left;

    private transient RemoteMatcher<IN> remoteMatcher;

    public RemoteStreamEquivalenceSink() {

    }

    public RemoteStreamEquivalenceSink(long matcherId, boolean left) {
        this.matcherId = matcherId;
        this.left = left;
    }

    public long getMatcherId() {
        return matcherId;
    }

    public void setMatcherId(long matcherId) {
        this.matcherId = matcherId;
    }

    public boolean isLeft() {
        return left;
    }

    public void setLeft(boolean left) {
        this.left = left;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Registry registry = LocateRegistry.getRegistry();
        RemoteMatcherRepository repository =
                (RemoteMatcherRepository) registry.lookup(RemoteMatcherFactory.REMOTE_MATCHER_REPOSITORY);
        remoteMatcher = repository.getMatcherById(matcherId);
    }

    @Override
    public void close() {
        remoteMatcher = null;
    }

    @Override
    public void invoke(IN item, Context context) throws Exception {
        remoteMatcher.processItem(item, left);
    }
}
