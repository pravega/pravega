/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor;

import com.emc.pravega.controller.server.ControllerServerException;

/**
 * Wrapper for exceptions thrown from Actor's preStart initialization hook.
 */
public class EventProcessorInitException extends ControllerServerException {

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
