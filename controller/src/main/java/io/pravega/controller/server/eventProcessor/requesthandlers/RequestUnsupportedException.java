/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor.requesthandlers;

import io.pravega.controller.server.ControllerServerException;

/**
 * Unsupported Request thrown when there is no requestHandler to process the request.
 */
public class RequestUnsupportedException extends ControllerServerException {

    public RequestUnsupportedException(Throwable t) {
        super(t);
    }

    public RequestUnsupportedException(String message) {
        super(message);
    }

    public RequestUnsupportedException(String message, Throwable t) {
        super(message, t);
    }
}
