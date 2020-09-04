package edu.upenn.diffstream.matcher;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteMatcherRepository extends Remote {

    String REMOTE_HOSTNAME = "localhost";
    String REMOTE_MATCHER_REPOSITORY = "RemoteMatcherRepository";

    <IN extends Serializable> RemoteMatcher<IN> getRemoteMatcher(long matcherId) throws RemoteException;

}
