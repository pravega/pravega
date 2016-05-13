package com.emc.logservice.Core;

import java.util.Enumeration;
import java.util.Iterator;

/**
 * Wraps an Iterator into an Enumeration.
 *
 * @param <T> The type of the elements in the enumeration.
 */
public class IteratorToEnumeration<T> implements Enumeration<T> {
    private final Iterator<T> iterator;

    public IteratorToEnumeration(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasMoreElements() {
        return this.iterator.hasNext();
    }

    @Override
    public T nextElement() {
        return this.iterator.next();
    }
}

