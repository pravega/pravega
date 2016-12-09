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
    private final Object lock = new Object();

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TruncateableList class.
     */
    public TruncateableList() {
        this.head = null;
        this.tail = null;
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
        synchronized (this.lock) {
            if (this.tail == null) {
                this.head = node;
            } else {
                this.tail.next = node;
            }

            this.tail = node;
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
        synchronized (this.lock) {
            if (this.tail == null) {
                // List is currently empty.
                this.head = node;
            } else {
                if (!lastItemChecker.test(this.tail.item)) {
                    // Test failed
                    return false;
                }

                this.tail.next = node;
            }

            this.tail = node;
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
        synchronized (this.lock) {
            // We truncate by finding the new head and simply pointing our head reference to it, as well as disconnecting
            // its predecessor node from it.
            // We also need to mark every truncated node as such - this will instruct ongoing reads to stop serving truncated data.
            ListNode<T> current = this.head;
            while (current != null && tester.test(current.item)) {
                current = trim(current);
                count++;
            }

            this.head = current;
            if (this.head == null) {
                this.tail = null;
            }

            this.size -= count;
        }

        return count;
    }

    /**
     * Clears the list.
     */
    public void clear() {
        synchronized (this.lock) {
            // Mark every node as truncated.
            ListNode<T> current = this.head;
            while (current != null) {
                current = trim(current);
            }

            // Clear the list.
            this.head = null;
            this.tail = null;
            this.size = 0;
        }
    }

    /**
     * Gets the first element in the list, if any.
     *
     * @return The first element, or null if the list is empty.
     */
    public T getFirst() {
        synchronized (this.lock) {
            return getNodeValue(this.head);
        }
    }

    /**
     * Gets the last element in the list, if any.
     *
     * @return The last element, or null if the list is empty.
     */
    public T getLast() {
        synchronized (this.lock) {
            return getNodeValue(this.tail);
        }
    }

    /**
     * Gets a value indicating the current size of the list.
     */
    public int size() {
        return this.size;
    }

    /**
     * Reads a number of items starting with the first one that matches the given predicate.
     *
     * @param firstItemTester A predicate that is used toe find the first item.
     * @param count           The maximum number of items to read.
     * @return An Iterator with the resulting items. If no results are available for the given parameters, an empty iterator is returned.
     */
    public Iterator<T> read(Predicate<T> firstItemTester, int count) {
        ListNode<T> firstNode = getFirstWithCondition(firstItemTester);
        return new NodeIterator<>(firstNode, count, this.lock);
    }

    /**
     * Gets the first node for which the given predicate returns true.
     */
    private ListNode<T> getFirstWithCondition(Predicate<T> firstItemTester) {
        ListNode<T> firstNode;
        synchronized (this.lock) {
            firstNode = this.head;
            while (firstNode != null && !firstItemTester.test(firstNode.item)) {
                firstNode = firstNode.next;
            }
        }

        return firstNode;
    }

    private T getNodeValue(ListNode<T> node) {
        if (node == null) {
            return null;
        } else {
            return node.item;
        }
    }

    private ListNode<T> trim(ListNode<T> node) {
        ListNode<T> next = node.next;
        node.next = null;
        node.truncated = true;
        return next;
    }

    //endregion

    //region ListNode

    private static class ListNode<T> {
        final T item;
        ListNode<T> next;
        boolean truncated;

        ListNode(T item) {
            this.item = item;
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
                // There is a scenarios where calling hasNext() returns true for the caller, but we end up in here.
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
                    // Store the current node in "resultNode" and advance the current node to the next one.
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
}
