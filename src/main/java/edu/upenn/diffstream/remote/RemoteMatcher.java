package edu.upenn.diffstream.remote;

import edu.upenn.diffstream.StreamsNotEquivalentException;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteMatcher<IN extends Serializable> extends Remote {

    void processItem(IN item, boolean left) throws RemoteException, StreamsNotEquivalentException;

}