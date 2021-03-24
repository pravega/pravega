/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.util;

import com.google.common.annotations.VisibleForTesting;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This class is a wrapper over async iterator that implements java.util.Iterator interface.
 * This is blocking wrapper on async iterator's getNext method which implements blocking hasNext and next methods of 
 * iterator interface. 
 * This class is threadsafe but that has the drawback of blocking on asyncIterator's getNext within a lock. 
 */
@ThreadSafe
@VisibleForTesting
public class BlockingAsyncIterator<T> implements Iterator<T> {
    private final AsyncIterator<T> asyncIterator;
    @GuardedBy("$lock")
    private T next;
    @GuardedBy("$lock")
    private boolean canHaveNext;

    public BlockingAsyncIterator(AsyncIterator<T> asyncIterator) {
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
    public T next() {
        load();
        if (next != null) {
            T retVal = next;
            next = null;
            return retVal;
        } else {
            assert !canHaveNext;
            throw new NoSuchElementException();
        }
    }
}
