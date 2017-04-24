/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream;

/**
 * Transaction heartbeat to a controller instance failed, because of one of the following reasons.
 * (1) Maximum transaction execution time exceeded,
 * (2) Scale grace period exceeded,
 * (3) Controller instance becomes unreachable
 */
public class PingFailedException extends Exception {

    public PingFailedException() {
        super();
    }

    public PingFailedException(Throwable t) {
        super(t);
    }

    public PingFailedException(String message, Throwable t) {
        super(message, t);
    }

    public PingFailedException(String message) {
        super(message);
    }
}
