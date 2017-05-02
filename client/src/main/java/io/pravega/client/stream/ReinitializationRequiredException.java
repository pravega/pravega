/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.client.stream;

public class ReinitializationRequiredException extends Exception {

    private static final long serialVersionUID = 1L;

    public ReinitializationRequiredException() {
        super();
    }

    public ReinitializationRequiredException(Throwable e) {
        super(e);
    }

    public ReinitializationRequiredException(String msg, Throwable e) {
        super(msg, e);
    }

    public ReinitializationRequiredException(String msg) {
        super(msg);
    }
}
