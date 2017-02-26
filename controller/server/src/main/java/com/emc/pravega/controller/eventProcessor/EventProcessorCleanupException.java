/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor;

import com.emc.pravega.controller.eventProcessor.impl.EventProcessor;

/**
 * Wrapper for exceptions thrown from Actor's postStop cleanup hook.
 */
public class EventProcessorCleanupException extends Exception {

    private final EventProcessor actor;

    EventProcessorCleanupException(final EventProcessor actor, final String message) {
        super(message);
        this.actor = actor;
    }

    EventProcessorCleanupException(final EventProcessor actor, final String message, final Throwable throwable) {
        super(message, throwable);
        this.actor = actor;
    }

    EventProcessorCleanupException(final EventProcessor actor, final Throwable throwable) {
        super(throwable);
        this.actor = actor;
    }

}
