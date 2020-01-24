/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;

/**
 * Represents a List that can append only on one end and can truncate from the other, which provides random reads.
 *
 * @param <T> The type of the list items.
 */
@ThreadSafe
public class SequencedItemList<T extends SequencedItemList.Element> {
    //region Members

    @GuardedBy("lock")
    private ListNode<T> head;
    @GuardedBy("lock")
    private ListNode<T> tail;
    private final Object lock = new Object();

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TruncateableList class.
     */
    public SequencedItemList() {
        this.head = null;
        this.tail = null;
    }

    //endregion

    //region Operations

    /**
     * Adds a new item at the end of the list, but only if the given item has a Sequence Number higher than the last
     * element in the list.
     *
     * @param item The item to append.
     * @return True if the item was added (meets sequencing criteria or list was empty), false otherwise.
     */
    public boolean add(T item) {
        ListNode<T> node = new ListNode<>(item);
        synchronized (this.lock) {
            if (this.tail == null) {
                // List is currently empty.
                this.head = node;
            } else {
                if (item.getSequenceNumber() <= this.tail.item.getSequenceNumber()) {
                    // Item to be added is not in order - reject it.
                    return false;
                }

                this.tail.next = node;
            }

            this.tail = node;
        }

        return true;
    }

    /**
     * Truncates items from the beginning of the list up to, and including, the element with the given Sequence Number.
     *
     * @param upToSequenceNumber The Sequence Number to truncate up to.
     * @return The number of truncated items.
     */
    public int truncate(long upToSequenceNumber) {
        int count = 0;
        synchronized (this.lock) {
            // We truncate by finding the new head and simply pointing our head reference to it, as well as disconnecting
            // its predecessor node from it. We also need to mark every truncated node as such - this will instruct ongoing
            // reads to stop serving truncated data.
            while (this.head != null && this.head.item.getSequenceNumber() <= upToSequenceNumber) {
                this.head = trim(this.head);
                count++;
            }

            if (this.head == null) {
                this.tail = null;
            }
        }

        return count;
    }

    /**
     * Clears the list.
     */
    public void clear() {
        synchronized (this.lock) {
            // Mark every node as truncated.
            while (this.head != null) {
                this.head = trim(this.head);
            }

            this.tail = null;
        }
    }

    /**
     * Gets the last element in the list, if any.
     *
     * @return The last element, or null if the list is empty.
     */
    public T getLast() {
        synchronized (this.lock) {
            return this.tail == null ? null : this.tail.item;
        }
    }

    /**
     * Reads a number of items starting with the first one that has a Sequence Number higher than the given one.
     *
     * @param afterSequenceNumber The sequence to search from.
     * @param count               The maximum number of items to read.
     * @return An Iterator with the resulting items. If no results are available for the given parameters, an empty iterator is returned.
     */
    public Iterator<T> read(long afterSequenceNumber, int count) {
        ListNode<T> firstNode;
        synchronized (this.lock) {
            firstNode = this.head;
        }

        // Find the first node that has a Sequence Number after the given one, but make sure we release and reacquire
        // the lock with every iteration. This will prevent long-list scans from blocking adds.
        while (firstNode != null && firstNode.item.getSequenceNumber() <= afterSequenceNumber) {
            synchronized (this.lock) {
                firstNode = firstNode.next;
            }
        }

        return new NodeIterator<>(firstNode, count, this.lock);
    }

    private ListNode<T> trim(ListNode<T> node) {
        ListNode<T> next = node.next;
        node.next = null;
        node.truncated = true;
        return next;
    }

    //endregion

    //region ListNode

    @RequiredArgsConstructor
    private static class ListNode<T> {
        final T item;
        ListNode<T> next;
        boolean truncated;

        @Override
        public String toString() {
            return this.item.toString();
        }
    }

    //endregion

    //region NodeIterator

    /**
     * An Iterator of Items in the list.
     *
     * @param <T> The type of the items in the list.
     */
    private static class NodeIterator<T> implements Iterator<T> {
        private ListNode<T> currentNode;
        private final int maxCount;
        private int countSoFar;
        private final Object lock;

        NodeIterator(ListNode<T> firstNode, int maxCount, Object lock) {
            Preconditions.checkArgument(maxCount >= 0, "maxCount must be a positive integer");

            this.currentNode = firstNode;
            this.maxCount = maxCount;
            this.lock = lock;
        }

        //region Iterator Implementation

        @Override
        public boolean hasNext() {
            return this.countSoFar < this.maxCount && this.currentNode != null && !this.currentNode.truncated;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                // There is a scenario where calling hasNext() returns true for the caller, but we end up in here.
                // If the current element has been truncated out after the user's call to hasNext() but before the call
                // to next(), we are forced to throw this exception because we cannot return a truncated element.
                throw new NoSuchElementException("No more elements left to iterate on.");
            }

            T result = this.currentNode.item;
            fetchNext();
            return result;
        }

        private void fetchNext() {
            synchronized (this.lock) {
                if (hasNext()) {
                    // We haven't exceeded our max count and we still have nodes to advance to.
                    this.currentNode = this.currentNode.next;
                    this.countSoFar++;
                } else {
                    // Either exceeded the max count or cannot advance anymore (truncated or end of list).
                    this.currentNode = null;
                }
            }
        }

        //endregion
    }

    //endregion

    /**
     * Defines an Element that can be added to a SequencedItemList.
     */
    public interface Element {
        /**
         * Gets a value indicating the Sequence Number for this item.
         * The Sequence Number is a unique, strictly monotonically increasing number that assigns order to items.
         *
         * @return Long indicating the Sequence number for this item.
         */
        long getSequenceNumber();
    }
}
