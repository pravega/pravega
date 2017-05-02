/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.eventProcessor;

import io.pravega.server.controller.service.server.ControllerServerException;

/**
 * Wrapper for exceptions thrown from Actor's preRestart reinitialization hook.
 */
public class EventProcessorReinitException extends ControllerServerException {

    public EventProcessorReinitException(final String message) {
        super(message);
    }

    public EventProcessorReinitException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public EventProcessorReinitException(final Throwable throwable) {
        super(throwable);
    }

}
