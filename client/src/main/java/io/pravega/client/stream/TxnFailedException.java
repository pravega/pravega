/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

/**
 * A transaction has failed. Usually because of it timed out or someone called
 * {@link Transaction#abort()}
 */
public class TxnFailedException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of TxnFailedException class.
     */
    public TxnFailedException() {
        super();
    }

    /**
     * Creates a new instance of TxnFailedException class.
     *
     * @param e The cause.
     */
    public TxnFailedException(Throwable e) {
        super(e);
    }

    /**
     * Creates a new instance of TxnFailedException class.
     *
     * @param msg Exception description.
     * @param e   The cause.
     */
    public TxnFailedException(String msg, Throwable e) {
        super(msg, e);
    }

    /**
     * Creates a new instance of TxnFailedException class.
     *
     * @param msg Exception description.
     */
    public TxnFailedException(String msg) {
        super(msg);
    }
}
