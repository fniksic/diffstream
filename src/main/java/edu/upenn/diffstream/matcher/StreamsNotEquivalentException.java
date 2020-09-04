package edu.upenn.diffstream.matcher;

public class StreamsNotEquivalentException extends Exception {

    public static boolean isRootCauseOf(Throwable e) {
        Throwable rootCause = e;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        return rootCause instanceof StreamsNotEquivalentException;
    }

}
