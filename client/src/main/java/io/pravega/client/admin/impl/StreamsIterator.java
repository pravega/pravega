/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.admin.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.stream.Stream;
import io.pravega.common.util.AsyncIterator;
import lombok.Synchronized;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This class is a wrapper over async iterator for iterating over streams.
 * This is blocking wrapper on async iterator's getNext method which implements blocking hasNext and next methods of 
 * iterator interface. 
 * We have also made this class threadsafe but that has the drawback of blocking on asyncIterator's getNext within a lock. 
 */
@ThreadSafe
@VisibleForTesting
class StreamsIterator implements Iterator<Stream> {
    private final AsyncIterator<Stream> asyncIterator;
    private Stream next;
    private boolean canHaveNext;
    
    StreamsIterator(AsyncIterator<Stream> asyncIterator) {
        this.canHaveNext = true;
        this.next = null;
        this.asyncIterator = asyncIterator;
    }

    @Synchronized
    private void load() {
        if (next == null && canHaveNext) {
            next = asyncIterator.getNext().join();
            if (next == null) {
                canHaveNext = false;
            }
        }
    }
    
    @Override
    @Synchronized
    public boolean hasNext() {
        load();
        return canHaveNext;
    }

    @Override
    @Synchronized
    public Stream next() {
        load();
        if (next != null) {
            Stream stream = next;
            next = null;
            return stream;
        } else {
            assert !canHaveNext;
            throw new NoSuchElementException();
        }
    }
}
