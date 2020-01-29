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
 * Transaction heartbeat to a controller instance failed, because of one of the following reasons.
 * (1) Maximum transaction execution time exceeded,
 * (2) Scale grace period exceeded,
 * (3) Controller instance becomes unreachable
 */
public class PingFailedException extends Exception {

    private static final long serialVersionUID = 1L;

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
