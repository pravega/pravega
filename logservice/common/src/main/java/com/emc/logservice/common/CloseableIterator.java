package com.emc.logservice.common;

import java.time.Duration;

/**
 * Defines an Iterator that can be closed.
 * This can be used for such iterators that need to acquire or make use of expensive system resources, such as network
 * connections or file handles. Closing the iterator will release all such resources, even if getNext() indicates that
 * it hasn't reached the end.
 */
public interface CloseableIterator<T, TEx extends Exception> extends AutoCloseable {
    /**
     * Gets the next item in the iteration.
     *
     * @param timeout The timeout for this operation.
     * @return The next item, or null if no more elements.
     * @throws ObjectClosedException If the CloseableIterator has been closed.
     * @throws TEx
     */
    T getNext(Duration timeout) throws TEx;

    /**
     * Closes the Iterator.
     */
    @Override
    void close();
}
