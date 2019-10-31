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
 * The message or sequence of messages that occurred make no sense and must be a result of a bug.
 */
public class InvalidMessageException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidMessageException() {
        super();
    }

    public InvalidMessageException(String string) {
        super(string);
    }

    public InvalidMessageException(Throwable throwable) {
        super(throwable);
    }
}
