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

import io.pravega.controller.retryable.RetryableException;
import lombok.AllArgsConstructor;

public class TaskExceptions {
    @AllArgsConstructor
    private static class StreamTaskException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final String message;
        private final Throwable cause;
    }

    /**
     * This Exception is thrown if the task has not been started yet but the event is picked up for processing.
     */
    public static class StartException extends StreamTaskException {
        public StartException(String message) {
            super(message, null);
        }
    }

    /**
     * This exception is thrown if we are unable to post the event into request stream.
     */
    public static class PostEventException extends StreamTaskException implements RetryableException {

        public PostEventException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * This exception is thrown if event processing is not enabled.
     */
    public static class ProcessingDisabledException extends StreamTaskException {
        public ProcessingDisabledException(String message) {
            super(message, null);
        }
    }
}
