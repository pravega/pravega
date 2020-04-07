/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.reading;

import io.pravega.segmentstore.contracts.ReadResultEntry;
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
     * @return The CompletionConsumer that was set using setCompletionCallback.
     */
    CompletionConsumer getCompletionCallback();

    /**
     * Attempts to fail the content request for this {@link ReadResultEntry} if in progress.
     *
     * @param ex The exception to fail with.
     * @throws IllegalStateException If {@link #isDone()} is true.
     */
    void fail(Throwable ex);

    /**
     * Gets a value indicating whether the content of this {@link ReadResultEntry} is readily available.
     *
     * @return True if available, false if not.
     */
    default boolean isDone() {
        return getContent().isDone();
    }

    @FunctionalInterface
    interface CompletionConsumer extends Consumer<Integer> {
    }
}
