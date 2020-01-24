/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
