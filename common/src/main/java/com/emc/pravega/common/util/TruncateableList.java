/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.common.util;

import com.emc.pravega.common.concurrent.AutoReleaseLock;
import com.emc.pravega.common.concurrent.ReadWriteAutoReleaseLock;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

/**
 * Represents a List that can append only on one end and can truncate from the other, which provides random reads.
 *
 * @param <T> The type of the list items.
 */
public class TruncateableList<T> {
    //region Members

    private ListNode<T> head;
    private ListNode<T> tail;
    private int size;
    private final ReadWriteAutoReleaseLock lock;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TruncateableList class.
     */
    public TruncateableList() {
        this.head = null;
        this.tail = null;
        this.lock = new ReadWriteAutoReleaseLock();
    }

    //endregion

    //region Operations

    /**
     * Adds a new item at the end of the list.
     *
     * @param item The item to append.
     */
    public void add(T item) {
        ListNode<T> node = new ListNode<>(item);
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            if (this.tail == null) {
                this.head = node;
                this.tail = node;
            } else {
                this.tail.setNext(node);
                this.tail = node;
            }

            this.size++;
        }
    }

    /**
     * Adds a new item at the end of the list, but only if the last item in the list meets the specified criteria.
     *
     * @param item            The item to append.
     * @param lastItemChecker A predicate to test the tail item in the list. If this predicate returns true, the item
     *                        is added at the end of the list, otherwise it is not.
     * @return True if the item was added (lastItemChecker returned true or list was empty), false otherwise.
     */
    public boolean addIf(T item, Predicate<T> lastItemChecker) {
        ListNode<T> node = new ListNode<>(item);
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            if (this.tail == null) {
                // List is currently empty.
                this.head = node;
                this.tail = node;
            } else {
                if (!lastItemChecker.test(this.tail.getItem())) {
                    // Test failed
                    return false;
                }

                this.tail.setNext(node);
                this.tail = node;
            }

            this.size++;
        }

        return true;
    }

    /**
     * Truncates items from the beginning of the list, as long as the given predicate returns true for the items.
     *
     * @param tester The predicate to use for testing.
     */
    public int truncate(Predicate<T> tester) {
        int count = 0;
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            // We truncate by finding the new head and simply pointing our head reference to it, as well as disconnecting
            // its predecessor node from it.
            // We also need to mark every truncated node as such - this will instruct ongoing reads to stop serving truncated data.
            ListNode<T> newHead = this.head;
            ListNode<T> prevNode = null;
            while (newHead != null && tester.test(newHead.item)) {
                newHead.markTruncated();
                prevNode = newHead;
                newHead = newHead.next;
                count++;
            }

            this.head = newHead;
            if (this.head == null) {
                this.tail = null;
            }

            if (prevNode != null) {
                prevNode.next = null;
            }

            this.size -= count;
        }

        return count;
    }

    /**
     * Clears the list.
     */
    public void clear() {
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            // Mark every node as truncated.
            ListNode<T> current = this.head;
            while (current != null) {
                current.markTruncated();
                current = current.next;
            }

            // Clear the list.
            this.head = null;
            this.tail = null;
            this.size = 0;
        }
    }

    /**
     * Gets a value indicating the current size of the list.
     *
     * @return The result.
     */
    public int getSize() {
        return this.size;
    }

    /**
     * Reads a number of items starting with the first one that matches the given predicate.
     *
     * @param firstItemTester A predicate that is used toe find the first item.
     * @param count           The maximum number of items to read.
     * @return An Enumeration of the resulting items. If no results are avaialable for the given parameters, an empty enumeration is returned.
     */
    public Iterator<T> read(Predicate<T> firstItemTester, int count) {
        ListNode<T> firstNode = this.getFirst(firstItemTester);
        return new NodeIterator<>(firstNode, count);
    }

    /**
     * Gets the first node for which the given predicate returns true.
     */
    private ListNode<T> getFirst(Predicate<T> firstItemTester) {
        ListNode<T> firstNode;
        try (AutoReleaseLock ignored = this.lock.acquireReadLock()) {
            firstNode = this.head;
            while (firstNode != null && !firstItemTester.test(firstNode.item)) {
                firstNode = firstNode.getNext();
            }
        }

        return firstNode;
    }

    //endregion

    //region ListNode

    /**
     * An individual node in the list.
     *
     * @param <T> The type of node contents.
     */
    private class ListNode<T> {
        private final T item;
        private ListNode<T> next;
        private boolean truncated;

        /**
         * Creates a new instance of the ListNode class with the given item as contents.
         *
         * @param item
         */
        public ListNode(T item) {
            this.item = item;
        }

        /**
         * Gets the contents of this node.
         *
         * @return The result
         */
        public T getItem() {
            return this.item;
        }

        /**
         * Gets a pointer to the next node in the list.
         *
         * @return The next node, or null if no such node exists.
         */
        public ListNode<T> getNext() {
            return this.next;
        }

        /**
         * Sets the next node in the list.
         *
         * @param next The next node in the list.
         */
        public void setNext(ListNode<T> next) {
            this.next = next;
        }

        /**
         * Indicates that this node has been truncated out of the list.
         */
        public void markTruncated() {
            this.truncated = true;
        }

        /**
         * Gets a value indicating whether this node has been truncated out of the list.
         *
         * @return
         */
        public boolean isTruncated() {
            return this.truncated;
        }

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
    private class NodeIterator<T> implements Iterator<T> {
        private ListNode<T> nextNode;
        private final int maxCount;
        private int countSoFar;

        /**
         * Creates a new instance of the NodeIterator class, starting with the given node.
         *
         * @param firstNode The first node in the iterator.
         * @param maxCount  The maximum number of items to return with the iterator.
         */
        public NodeIterator(ListNode<T> firstNode, int maxCount) {
            Preconditions.checkArgument(maxCount >= 0, "maxCount");

            this.nextNode = firstNode;
            this.maxCount = maxCount;
        }

        //region Iterator Implementation

        @Override
        public boolean hasNext() {
            return this.countSoFar < this.maxCount && this.nextNode != null && !this.nextNode.isTruncated();
        }

        @Override
        public T next() {
            T result;
            if (this.countSoFar >= this.maxCount || this.nextNode == null) {
                throw new NoSuchElementException("Reached the end of the enumeration.");
            } else if (this.nextNode.isTruncated()) {
                throw new NoSuchElementException("List has been truncated and current read may not continue.");
            } else {
                result = this.nextNode.item;
                this.nextNode = this.nextNode.getNext();
                this.countSoFar++;
            }

            return result;
        }

        //endregion
    }

    //endregion
}
