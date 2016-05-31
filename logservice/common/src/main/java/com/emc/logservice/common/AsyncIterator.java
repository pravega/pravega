package com.emc.logservice.common;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an Iterator that returns items asynchronously.
 */
public interface AsyncIterator<T> extends AutoCloseable {
    /**
     * Gets the next item in the iteration.
     * @param timeout The timeout for this operation.
     * @return The next item, or null if no more elements.
     * @throws ObjectClosedException If the AsyncIterator has been closed.
     */
    CompletableFuture<T> getNext(Duration timeout);

    /**
     * Closes the Iterator.
     */
    @Override
    void close();
}
