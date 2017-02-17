/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

/**
 * A transaction has failed. Usually because of it timed out or someone called
 * {@link Transaction#abort()}
 */
public class TxnFailedException extends Exception {

    private static final long serialVersionUID = 1L;

    public TxnFailedException() {
        super();
    }

    public TxnFailedException(Throwable e) {
        super(e);
    }

    public TxnFailedException(String msg, Throwable e) {
        super(msg, e);
    }

    public TxnFailedException(String msg) {
        super(msg);
    }
}
