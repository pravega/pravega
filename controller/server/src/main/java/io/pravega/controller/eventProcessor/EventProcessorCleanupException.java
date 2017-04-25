/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.eventProcessor;

import io.pravega.controller.eventProcessor.impl.EventProcessor;
import lombok.Getter;

/**
 * Wrapper for exceptions thrown from Actor's postStop cleanup hook.
 */
public class EventProcessorCleanupException extends Exception {
    private static final long serialVersionUID = 1L;
    
    @Getter
    private final EventProcessor<?> actor;

    EventProcessorCleanupException(final EventProcessor<?> actor, final String message) {
        super(message);
        this.actor = actor;
    }

    EventProcessorCleanupException(final EventProcessor<?> actor, final String message, final Throwable throwable) {
        super(message, throwable);
        this.actor = actor;
    }

    EventProcessorCleanupException(final EventProcessor<?> actor, final Throwable throwable) {
        super(throwable);
        this.actor = actor;
    }

}
