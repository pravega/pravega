/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.reading;

import com.emc.pravega.service.contracts.ReadResultEntry;

import java.util.function.Consumer;

/**
 * Extends the ReadResultEntry interface by adding the ability to register a callback to be invoked upon completion.
 */
interface CompletableReadResultEntry extends ReadResultEntry {
    /**
     * Registers a CompletionConsumer that will be invoked when the content is retrieved, just before the Future is completed.
     *
     * @param completionCallback The callback to be invoked.
     */
    void setCompletionCallback(CompletionConsumer completionCallback);

    /**
     * Gets the CompletionConsumer that was set using setCompletionCallback.
     */
    CompletionConsumer getCompletionCallback();

    @FunctionalInterface
    interface CompletionConsumer extends Consumer<Integer> {
    }
}
