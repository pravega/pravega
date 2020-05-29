/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

public class ControllerFailureException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ControllerFailureException(String message) {
        super(message);
    }

    public ControllerFailureException(Throwable e) {
        super(e);
    }

    public ControllerFailureException(String message, Throwable e) {
        super(message, e);
    }

}
