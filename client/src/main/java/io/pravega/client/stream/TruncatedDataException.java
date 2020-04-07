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

public class TruncatedDataException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    
    /**
     * Creates a new instance of TruncatedDataException class.
     */
    public TruncatedDataException() {
        super();
    }

    /**
     * Creates a new instance of TruncatedDataException class.
     *
     * @param e The cause.
     */
    public TruncatedDataException(Throwable e) {
        super(e);
    }

    /**
     * Creates a new instance of TruncatedDataException class.
     *
     * @param msg Exception description.
     * @param e   The cause.
     */
    public TruncatedDataException(String msg, Throwable e) {
        super(msg, e);
    }

    /**
     * Creates a new instance of TruncatedDataException class.
     *
     * @param msg Exception description.
     */
    public TruncatedDataException(String msg) {
        super(msg);
    }
}
