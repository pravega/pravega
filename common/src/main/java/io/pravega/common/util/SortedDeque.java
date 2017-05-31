/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import com.google.common.base.Preconditions;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Deque-like data structure that accepts elements in sorted order and provides optimized operations based on that.
 * Note: This class was inspired by java.util.ArrayDeque, however that class does not allow subclassing outside of its package.
 */
@NotThreadSafe
@SuppressWarnings("unchecked")
public class SortedDeque<E extends SortedIndex.IndexEntry> {
    //region Members

    private transient SortedIndex.IndexEntry[] elements;
    private transient int lastSlotIndex;
    private transient int head;
    private transient int tail;
    private static final int MIN_INITIAL_CAPACITY = 8;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SortedDeque class.
     */
    public SortedDeque() {
        this(MIN_INITIAL_CAPACITY);
    }

    /**
     * Creates a new instance of the SortedDeque class.
     *
     * @param initialCapacity The initial capacity.
     */
    public SortedDeque(int initialCapacity) {
        this.elements = new SortedIndex.IndexEntry[Math.max(MIN_INITIAL_CAPACITY, initialCapacity)];
        this.lastSlotIndex = this.elements.length - 1;
    }

    //endregion

    //region Operations

    /**
     * Adds an item to the end of the Deque.
     *
     * @param item The Item to add
     * @throws IllegalArgumentException If the item's key is not greater than the key of the last item in the Deque.
     */
    public void addLast(E item) {
        SortedIndex.IndexEntry last = this.elements[(this.tail - 1) & this.lastSlotIndex];
        Preconditions.checkArgument(isEmpty() || item.key() > last.key(), "keys must be in increasing order.");
        this.elements[this.tail] = item;
        this.tail = (this.tail + 1) & this.lastSlotIndex;
        if (this.tail == this.head) {
            doubleCapacity();
        }
    }

    /**
     * Removes the first item in the Deque.
     *
     * @return The first item in the Deque, or null if Deque is empty.
     */
    public E removeFirst() {
        E result = (E) this.elements[this.head]; // Element is null if deque empty
        if (result == null) {
            return null;
        }

        // Drop the pointer to the result and move the head over by one slot.
        this.elements[this.head] = null;
        this.head = (this.head + 1) & this.lastSlotIndex;
        return result;
    }

    /**
     * Removes all the items from the beginning of the Deque up to and including the given item, but ONLY if the given
     * item exists in the Deque.
     *
     * @param upToIncluding The Item to remove up to and including. If this item does not exist, no change is made.
     * @return The last removed item, which has a key equal to that of upToIncluding.
     */
    public E removeFirst(E upToIncluding) {
        if ((this.head != this.tail) && upToIncluding.key() == this.elements[this.head].key()) {
            // Quick check: our desired item is the first one; don't bother doing a binary search.
            return removeFirst();
        }

        int itemIndex = binarySearch(upToIncluding.key());
        if (itemIndex < 0) {
            //Item does not exist, as such we cannot remove anything.
            return null;
        }

        // Remove all items from the beginning, up to and including our sought item.
        SortedIndex.IndexEntry result = null;
        while (itemIndex-- >= 0) {
            result = this.elements[this.head];
            this.elements[this.head] = null;
            this.head = (this.head + 1) & this.lastSlotIndex;
        }

        return (E) result;
    }

    /**
     * Removes the last item in the Deque.
     *
     * @return The last item in the Deque, or null if Deque is empty.
     */
    public E removeLast() {
        int t = (this.tail - 1) & this.lastSlotIndex;
        E result = (E) this.elements[t];
        if (result == null) {
            return null;
        }

        this.elements[t] = null;
        this.tail = t;
        return result;
    }

    /**
     * Removes all the items from the end of the Deque up to and including the given item, but ONLY if the given item
     * exists in the Deque.
     *
     * @param fromIncluding The Item to remove up to and including. If this item does not exist, no change is made.
     * @return The last removed item, which has a key equal to that of fromIncluding.
     */
    public E removeLast(E fromIncluding) {
        if ((this.head != this.tail) && fromIncluding.key() == this.elements[(this.tail - 1) & this.lastSlotIndex].key()) {
            // Quick check: our desired item is the last one; don't bother doing a binary search.
            return removeLast();
        }

        int itemIndex = binarySearch(fromIncluding.key());
        if (itemIndex < 0) {
            // Item does not exist, as such we cannot remove anything.
            return null;
        }

        // Remove all items from the end, up to and including our sought item.
        SortedIndex.IndexEntry result = null;
        int size = size();
        while (itemIndex++ < size) {
            int t = (this.tail - 1) & this.lastSlotIndex;
            result = this.elements[t];
            this.elements[t] = null;
            this.tail = t;
        }

        return (E) result;
    }

    /**
     * Returns the first item in the Deque, without removing it.
     *
     * @return The first item in the Deque, or null if Deque is empty.
     */
    public E peekFirst() {
        return (E) this.elements[this.head];
    }

    /**
     * Returns the last item in the Deque, without removing it.
     *
     * @return The last item in the Deque, or null if Deque is empty.
     */
    public E peekLast() {
        return (E) this.elements[(this.tail - 1) & this.lastSlotIndex];
    }

    /**
     * Clears the entire Deque.
     */
    public void clear() {
        if (this.head != this.tail) {
            do {
                this.elements[this.head] = null;
                this.head = (this.head + 1) & this.lastSlotIndex;
            } while (this.head != this.tail);
            this.head = this.tail = 0;
        }
    }

    /**
     * Returns the number of elements in this Deque.
     *
     * @return The result.
     */
    public int size() {
        return (this.tail - this.head) & this.lastSlotIndex;
    }

    /**
     * Returns true if this Deque contains no elements.
     *
     * @return The result.
     */
    public boolean isEmpty() {
        return this.head == this.tail;
    }

    @Override
    public String toString() {
        return String.format("Size = %d, Capacity = %d", size(), this.elements.length);
    }

    //endregion

    //region Helpers

    /**
     * Doubles the capacity of the Deque by creating a new array double of the current size and copying all elements into
     * it.
     */
    private void doubleCapacity() {
        assert this.head == this.tail : "not at capacity";
        int oldHead = this.head;
        int oldLength = this.elements.length;
        int r = oldLength - oldHead; // number of elements to the right of oldHead
        int newCapacity = oldLength << 1;
        Preconditions.checkState(newCapacity >= 0, "Deque too large.");

        SortedIndex.IndexEntry[] newElements = new SortedIndex.IndexEntry[newCapacity];
        System.arraycopy(this.elements, oldHead, newElements, 0, r);
        System.arraycopy(this.elements, 0, newElements, r, oldHead);
        this.elements = newElements;
        this.lastSlotIndex = this.elements.length - 1;
        this.head = 0;
        this.tail = oldLength;
    }

    /**
     * Performs a binary search in the Deque for an item whose key matches the given one.
     *
     * @param key The key to search.
     * @return The index in the element in the Deque (NOT the index in the base array), or -1 if no such element exists.
     */
    private int binarySearch(long key) {
        int lowIndex = 0;
        int highIndex = size() - 1;
        while (lowIndex <= highIndex) {
            int midIndex = (lowIndex + highIndex) / 2;
            SortedIndex.IndexEntry mid = this.elements[(this.head + midIndex) & this.lastSlotIndex];
            if (mid.key() < key) {
                lowIndex = midIndex + 1;
            } else if (mid.key() > key) {
                highIndex = midIndex - 1;
            } else {
                return midIndex;
            }
        }

        return -1;
    }

    //endregion
}