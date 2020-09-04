package edu.upenn.diffstream.matcher;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteMatcher<IN extends Serializable> extends Remote {

    void processItem(IN item, boolean left) throws RemoteException, StreamsNotEquivalentException;

}