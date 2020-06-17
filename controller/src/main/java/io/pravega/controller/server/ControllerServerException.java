/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

/**
 * Base class for exceptions thrown from controller server code base.
 */
public class ControllerServerException extends Exception {

    public ControllerServerException(Throwable t) {
        super(t);
    }

    public ControllerServerException(String message) {
        super(message);
    }

    public ControllerServerException(String message, Throwable t) {
        super(message, t);
    }
}
