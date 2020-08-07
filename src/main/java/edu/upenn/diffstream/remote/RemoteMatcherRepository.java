package edu.upenn.diffstream.remote;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteMatcherRepository extends Remote {

    <IN extends Serializable> RemoteMatcher<IN> getMatcherById(long matcherId) throws RemoteException;

}
