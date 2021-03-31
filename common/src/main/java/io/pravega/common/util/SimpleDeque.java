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

import com.google.common.base.Preconditions;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Simplified {@link java.util.Deque} implementation that provides an efficient method to remove multiple items at once
 * from the head.
 *
 * Note that this class does not implement {@link java.util.Deque} and as such not all methods in that interface are
 * present here. The only reason it exists is because there is no way in {@link java.util.ArrayDeque} to efficiently
 * remove multiple items at once (see {@link #pollFirst(int)}.
 *
 * @param <T> Type of item in the {@link SimpleDeque}.
 */
@NotThreadSafe
public class SimpleDeque<T> {
    //region Members

    private static final int MAX_CAPACITY = Integer.MAX_VALUE - Long.BYTES;
    private static final int MAX_HALF = MAX_CAPACITY / 2;
    private Object[] items;
    private int head;
    private int tail;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link SimpleDeque} class.
     */
    public SimpleDeque() {
        this(16);
    }

    /**
     * Creates a new instance of the {@link SimpleDeque} class.
     *
     * @param initialCapacity The initial capacity.
     */
    public SimpleDeque(int initialCapacity) {
        Preconditions.checkArgument(initialCapacity >= 0, "initialCapacity must be a non-negative number.");
        this.items = new Object[initialCapacity + 1];
    }

    //endregion

    //region Operations

    /**
     * Adds a new item at the tail of the {@link SimpleDeque}.
     * See {@link java.util.Deque#addLast}.
     *
     * @param item The item to add. Must be non-null (since we use null internally as a sentinel)
     */
    public void addLast(@NonNull T item) {
        this.items[this.tail] = item;
        this.tail = increment(this.tail);
        if (this.head == this.tail) {
            // Head == Tail means we are now full. Need to expand.
            expand();
        }
    }

    /**
     * Removes one item from the head of the {@link SimpleDeque}.
     * See {@link java.util.Deque#pollFirst()}.
     *
     * @return The item at the head of the {@link SimpleDeque}, or null of empty.
     */
    @SuppressWarnings("unchecked")
    public T pollFirst() {
        T e = (T) this.items[this.head];
        if (e != null) {
            this.items[this.head] = null;
            this.head = increment(this.head);
        }

        return e;
    }

    /**
     * Removes one or more items from the head of the {@link SimpleDeque}.
     *
     * This method is equivalent to:
     * <pre><code>
     *     SimpleDeque source;
     *     Queue target;
     *     int count = maxCount;
     *     while(count &gt; 0 &amp;&amp; !source.isEmpty()) {
     *         target.add(source.removeFirst());
     *         count--;
     *     }
     * </code></pre>
     *
     * This method is optimized for a bulk copy and is preferred to using the code exemplified above.
     *
     * @param maxCount The maximum number of items to remove. If this number is larger than {@link #size()}, then
     *                 {@link #size()} items will be removed.
     * @return A {@link Queue} containing the removed items, in the same order as they were in the {@link SimpleDeque}.
     */
    public Queue<T> pollFirst(int maxCount) {
        int count = Math.min(maxCount, size());
        final Object[] result = new Object[count];
        if (count == 0) {
            return new WrapQueue<>(result);
        }

        int copyLength = Math.min(this.items.length - this.head, count);
        System.arraycopy(this.items, this.head, result, 0, copyLength);
        Arrays.fill(this.items, this.head, this.head + copyLength, null);
        this.head = this.head + copyLength;
        if (this.head == this.items.length) {
            this.head = 0;
        }

        count -= copyLength;
        if (count > 0) {
            System.arraycopy(this.items, 0, result, copyLength, count);
            Arrays.fill(this.items, 0, count, null);
            this.head += count; // count is less than size(), so this can't wrap around no matter what.
        }

        return new WrapQueue<>(result);
    }

    /**
     * Returns (but does not remove) the item at the head of the {@link SimpleDeque}.
     * This method does not alter the internal state of the {@link SimpleDeque}.
     * See {@link java.util.Deque#peekFirst()}.
     *
     * @return The item at the head of the {@link SimpleDeque}, or null if empty.
     */
    @SuppressWarnings("unchecked")
    public T peekFirst() {
        return (T) this.items[this.head];
    }

    /**
     * Gets a value indicating the number of items in the {@link SimpleDeque}.
     *
     * @return The size of the {@link SimpleDeque}.
     */
    public int size() {
        final int size = this.tail - this.head;
        return size < 0 ? size + this.items.length : size;
    }

    /**
     * Gets a value indicating whether the {@link SimpleDeque} is empty or not.
     * Consider using this instead of {@code {@link #size()} == 0} since this method does not need to compute anything.
     *
     * @return True if empty, false otherwise.
     */
    public boolean isEmpty() {
        return this.head == this.tail;
    }

    /**
     * Clears the entire {@link SimpleDeque}.
     */
    public void clear() {
        Arrays.fill(this.items, null);
        this.head = 0;
        this.tail = 0;
    }

    private int increment(int index) {
        index++;
        return index == this.items.length ? 0 : index;
    }

    private void expand() {
        int newCapacity = this.items.length;
        if (this.items.length >= MAX_CAPACITY) {
            // Can't allocate an array bigger than this.
            throw new OutOfMemoryError("Unable to grow SimpleDequeue.");
        }

        if (newCapacity >= MAX_HALF) {
            // We double capacity, so if we're already above half of max, then just set it to max.
            newCapacity = MAX_CAPACITY;
        } else {
            // Double capacity.
            newCapacity = newCapacity * 2;
        }

        final Object[] newItems = new Object[newCapacity];
        if (this.head < this.tail) {
            // The items do not wrap around the end. We can do everything with a single copy.
            final int size = this.tail - this.head;
            System.arraycopy(this.items, this.head, newItems, 0, size);
            this.tail = size;
        } else {
            // The items wrap around the end. First copy from the head to the end, then from the beginning to the tail.
            final int copyLength = this.items.length - this.head;
            System.arraycopy(this.items, this.head, newItems, 0, copyLength);
            System.arraycopy(this.items, 0, newItems, copyLength, this.tail);
            this.tail = copyLength + this.tail;
        }

        this.head = 0;
        this.items = newItems;
        assert this.head != this.tail;
    }

    @Override
    public String toString() {
        return String.format("Size = %s, Capacity = %s, Head = %s, Tail = %s", size(), this.items.length, this.head, this.tail);
    }

    //endregion

    //region WrapQueue

    /**
     * Array-backed {@link Queue} implementation that only supports removing items. Items will be removed (via
     * {@link #poll()}, {@link #remove()}) in the order in which they appear in the array. Iterating (via {@link
     * #iterator()}) will return all the items that are left in the {@link Queue}, but it will not remove them.
     *
     * @param <T> Type of the items.
     */
    @RequiredArgsConstructor
    @NotThreadSafe
    private static class WrapQueue<T> extends AbstractCollection<T> implements Queue<T> {
        private final Object[] items;
        private int nextIndex = 0;

        //region Queue Implementation

        @Override
        @SuppressWarnings("unchecked")
        public T poll() {
            if (this.nextIndex >= this.items.length) {
                return null;
            }

            return (T) this.items[this.nextIndex++];
        }

        @Override
        public T element() {
            T result = peek();
            if (result == null) {
                throw new NoSuchElementException();
            }

            return result;
        }

        @Override
        @SuppressWarnings("unchecked")
        public T peek() {
            return this.nextIndex >= this.items.length ? null : (T) this.items[nextIndex];
        }

        @Override
        @SuppressWarnings("unchecked")
        public Iterator<T> iterator() {
            return Arrays.stream(this.items).skip(this.nextIndex).map(o -> (T) o).iterator();
        }

        @Override
        public int size() {
            return this.items.length - this.nextIndex;
        }

        @Override
        public T remove() {
            T result = poll();
            if (result == null) {
                throw new NoSuchElementException();
            }
            return result;
        }

        //endregion

        //region Unsupported Methods

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean offer(T t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        //endregion
    }

    //endregion
}

