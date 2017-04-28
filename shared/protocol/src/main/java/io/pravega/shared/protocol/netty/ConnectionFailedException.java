/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.shared.protocol.netty;

/**
 * The connection has failed, and needs to be re-established.
 *
 */
public class ConnectionFailedException extends Exception {

    private static final long serialVersionUID = 1L;

    public ConnectionFailedException() {
        super();
    }

    public ConnectionFailedException(String string) {
        super(string);
    }

    public ConnectionFailedException(Throwable throwable) {
        super(throwable);
    }

}
