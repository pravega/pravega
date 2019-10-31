/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.eventProcessor;

import io.pravega.controller.server.ControllerServerException;

/**
 * Wrapper for exceptions thrown from Actor's preStart initialization hook.
 */
public class EventProcessorInitException extends ControllerServerException {

    private static final long serialVersionUID = 1L;

    public EventProcessorInitException(final String message) {
        super(message);
    }

    public EventProcessorInitException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public EventProcessorInitException(final Throwable throwable) {
        super(throwable);
    }

}
