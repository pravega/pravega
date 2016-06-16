package com.emc.logservice.common;

import java.util.Iterator;

/**
 * Extension to Iterator that throws a checked exception.
 *
 * @param <E>  The type of the elements in the Iterator.
 * @param <Ex> The type of the exception that can be thrown.
 */
public interface IteratorWithException<E, Ex extends Throwable> extends Iterator<E> {
    /**
     * Attempts to retrieve the next element.
     *
     * @return The next element.
     * @throws Ex An exception that can be thrown if the next element cannot be retrieved (but there are more elements.
     */
    E pollNext() throws Ex;
}
