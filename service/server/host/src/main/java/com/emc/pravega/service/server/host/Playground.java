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

package com.emc.pravega.service.server.host;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.common.base.Preconditions;
import lombok.val;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Stack;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * Playground Test class.
 */
public class Playground {

    public static void main(String[] args) throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.INFO);
        //context.reset();

        int count = 500000;
        TreeMap<Integer, TestEntry> tm = new TreeMap<>();
        val testTree = new AvlTreeIndex<Integer, TestEntry>(Integer::compare);
        testTree.get(Integer.MAX_VALUE);
        //
        //        long tmInsertElapsed = measure(() -> insert(tm, 0, count));
        //        long tmReadElapsed = measure(() -> readExact(tm, count, count));
        //        long tmReadCeilingElapsed = measure(() -> readCeiling(tm, 0, count));
        //
        //        long testInsertElapsed = measure(() -> insert(testTree, 0, count));
        //        long testReadElapsed = measure(() -> readExact(testTree, 0, count));
        //        long testReadCeilingElapsed = measure(() -> readCeiling(testTree, 0, count));
        //
        //        tmInsertElapsed += measure(() -> insert(tm, count, count));
        //        tmReadElapsed += measure(() -> readExact(tm, count, count));
        //        tmReadCeilingElapsed += measure(() -> readCeiling(tm, count, count));
        //
        //        testInsertElapsed += measure(() -> insert(testTree, count, count));
        //        testReadElapsed += measure(() -> readExact(testTree, count, count));
        //        testReadCeilingElapsed += measure(() -> readCeiling(testTree, count, count));
        //
        //        System.out.println(String.format("Test_insert = %s ms, TM_insert = %sms; Test_read = %sms, TM_read = %sms; Test_ceiling = %sms, TM_ceiling = %sms",
        //                testInsertElapsed, tmInsertElapsed, testReadElapsed, tmReadElapsed, testReadCeilingElapsed, tmReadCeilingElapsed));

        int testCount = 100;
        int skip = 1;
        for (int i = 0; i < testCount; i += skip) {
            tm.put(i, new TestEntry(i));
            testTree.insert(new TestEntry(i));
        }
        for (int i = 0; i < testCount; i++) {
            val en = tm.ceilingEntry(i);
            val av = testTree.getCeiling(i);
            if ((en != null && av == null)
                    || (en == null && av != null)
                    || (en != null && en.getKey() != av.key())) {
                System.out.println(String.format("Key = %s, E = %s, A = %s", i, en, av));
            }
        }

        //        for (int i = 0; i < testCount; i++) {
        //            if (i % 2 == 0) {
        //                testTree.remove(i);
        //            }
        //        }

        for (int i = 0; i < testCount; i += skip) {
            if (i % 2 == 0) {
                testTree.remove(i);
                System.out.print(i + ":");
                testTree.forEach(e -> System.out.print(" " + e));
                System.out.println();
            }
        }

        //        testTree.forEach(e -> System.out.print(" " + e));
        System.out.println();
    }

    private static void insert(AvlTreeIndex<Integer, TestEntry> rbt, int start, int count) {
        int max = start + count;
        for (int i = start; i < max; i++) {
            rbt.insert(new TestEntry(i));
        }
    }

    private static void readExact(AvlTreeIndex<Integer, TestEntry> rbt, int start, int count) {
        int max = start + count;
        for (int i = start; i < max; i++) {
            val r = rbt.get(i);
            if (r.key != i) {
                System.out.println(r);
            }
        }
    }

    private static void readCeiling(AvlTreeIndex<Integer, TestEntry> rbt, int start, int count) {
        int max = start + count;
        for (int i = start; i < max; i++) {
            val r = rbt.getCeiling(i);
            if (r.key != i) {
                System.out.println(r);
            }
        }
    }

    private static void insert(TreeMap<Integer, TestEntry> tm, int start, int count) {
        int max = start + count;
        for (int i = start; i < max; i++) {
            tm.put(i, new TestEntry(i));
        }
    }

    private static void readExact(TreeMap<Integer, TestEntry> tm, int start, int count) {
        int max = start + count;
        for (int i = start; i < max; i++) {
            tm.get(i);
        }
    }

    private static void readCeiling(TreeMap<Integer, TestEntry> tm, int start, int count) {
        int max = start + count;
        for (int i = start; i < max; i++) {
            tm.ceilingEntry(i);
        }
    }

    private static int measure(Runnable r) {
        long rbtStart = System.nanoTime();
        r.run();
        return (int) ((System.nanoTime() - rbtStart) / 1000 / 1000);
    }

    static class TestEntry implements IndexEntry<Integer> {
        final Integer key;

        TestEntry(Integer key) {
            this.key = key;
        }

        @Override
        public Integer key() {
            return this.key;
        }

        @Override
        public String toString() {
            return Integer.toString(this.key);
        }
    }

    /**
     * Defines an Index that orders its IndexEntries by a Key.
     *
     * @param <K> The type of the Key.
     * @param <T> The type of the IndexEntries.
     */
    interface SortedIndex<K, T extends IndexEntry<K>> {
        /**
         * Clears the contents of the Index.
         */
        void clear();

        /**
         * Inserts the given item into the Index. If there already exists an item with the same key, it will be overridden.
         *
         * @param item The item to insert.
         * @return The displaced item, if any.
         */
        T insert(T item);

        /**
         * Removes any item with the given key from the Index.
         *
         * @param key The key of the item to remove.
         * @return The removed item, or null if nothing was removed.
         */
        T remove(K key);

        /**
         * Gets a value indicating the number of items in the Index.
         */
        int size();

        /**
         * Gets an item with the given key.
         *
         * @param key The key to search by.
         * @return The sought item, or null if item with the given key exists.
         */
        T get(K key);

        /**
         * Gets an item with the given key.
         *
         * @param key          The key to search by.
         * @param defaultValue The value to return if item with the given key exists.
         * @return The sought item, or defaultValue if no item with the given key exists.
         */
        T get(K key, T defaultValue);

        /**
         * Gets the smallest item whose key is greater than or equal to the given key.
         *
         * @param key the Key to search by.
         * @return The sought item, or null if no such item exists.
         */
        T getCeiling(K key);

        /**
         * Gets the smallest item whose key is greater than or equal to the given key.
         *
         * @param key          the Key to search by.
         * @param defaultValue The value to return if item with the given key exists.
         * @return The sought item, or defaultValue if no such item exists.
         */
        T getCeiling(K key, T defaultValue);

        /**
         * Gets the smallest item in the index, or null if index is empty.
         */
        T getFirst();

        /**
         * Gets the largest item in the index, or null if index is empty.
         */
        T getLast();

        /**
         * Iterates through each item in the Index, in natural order, and calls the given consumer on all of them.
         *
         * @param consumer The consumer to invoke.
         */
        void forEach(Consumer<T> consumer);
    }

    /**
     * Defines a generic entry into an Index.
     *
     * @param <K> The Type of the key.
     */
    interface IndexEntry<K> {
        /**
         * Gets a value representing the key of the entry. The Key should not change for the lifetime of the entry and
         * should be very cheap to return (as it is used very frequently).
         */
        K key();
    }

    static class AvlTreeIndex<K, T extends IndexEntry<K>> implements SortedIndex<K, T> {
        //region Members

        private static final int MAX_IMBALANCE = 1;
        private final Comparator<K> comparator;
        private Node<T> root;
        private int size;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the AvlTreeIndex class.
         *
         * @param comparator A Key comparator to use as orderer within the index.
         */
        AvlTreeIndex(Comparator<K> comparator) {
            Preconditions.checkNotNull(comparator, "comparator");
            this.comparator = comparator;
            this.root = null;
            this.size = 0;
        }

        //endregion

        //region SortedIndex Implementation

        @Override
        public void clear() {
            this.root = null;
            this.size = 0;
        }

        @Override
        public T insert(T item) {
            Preconditions.checkNotNull(item, "item");
            Preconditions.checkNotNull(item.key(), "item.key()");
            val result = insert(item, this.root);
            this.root = result.node;
            return result.updatedItem;
        }

        @Override
        public T remove(K key) {
            Preconditions.checkNotNull(key, "key");
            val result = remove(key, this.root);
            this.root = result.node;
            return result.updatedItem;
        }

        @Override
        public int size() {
            return this.size;
        }

        @Override
        public T get(K key) {
            return get(key, null);
        }

        @Override
        public T get(K key, T defaultValue) {
            // No need to check key != null, since we do not risk corrupting the tree with input is not valid.
            Node<T> node = this.root;
            while (node != null) {
                int compareResult = this.comparator.compare(key, node.item.key());
                if (compareResult < 0) {
                    node = node.left;
                } else if (compareResult > 0) {
                    node = node.right;
                } else {
                    return node.item;
                }
            }

            return defaultValue;
        }

        @Override
        public T getCeiling(K key) {
            return getCeiling(key, null);
        }

        @Override
        public T getCeiling(K key, T defaultValue) {
            // No need to check key != null, since we do not risk corrupting the tree with input is not valid.
            Node<T> node = this.root;
            Node<T> lastLeftChildParent = null;
            while (node != null) {
                // Compare the key to the current node's item.
                int compareResult = this.comparator.compare(key, node.item.key());
                if (compareResult < 0) {
                    // Sought key is smaller than the current node's item.
                    if (node.left == null) {
                        // No more nodes to the left, so this is the smallest item with a key greater than the given one.
                        return node.item;
                    }

                    // The left child has a key smaller than this one; Search again there.
                    lastLeftChildParent = node;
                    node = node.left;
                } else if (compareResult > 0) {
                    // Sought key is greater than the current node's item.
                    if (node.right != null) {
                        // Search again to the right (where we'll have a larger key).
                        node = node.right;
                    } else {
                        // There are no more nodes to the right - this is the node with the largest item in the Tree.
                        // If we had a pointer back to the parent, we would have to walk up the tree path as long as we
                        // have a parent and the current node is a right child, then return the parent of that node.
                        // In other words, if there exists a result, it is the parent of the last node that was selected as a
                        // left child, which is conveniently stored in lastLeftChildParent.
                        if (lastLeftChildParent != null) {
                            return lastLeftChildParent.item;
                        } else {
                            return defaultValue;
                        }
                    }
                } else {
                    // Exact match; return it.
                    return node.item;
                }
            }

            // Nothing could be found.
            return defaultValue;
        }

        @Override
        public T getFirst() {
            if (this.size == 0) {
                return null;
            }

            return findSmallest(this.root).item;
        }

        @Override
        public T getLast() {
            if (this.size == 0) {
                return null;
            }

            return getMax(this.root).item;
        }

        @Override
        public void forEach(Consumer<T> consumer) {
            Preconditions.checkNotNull(consumer, "consumer");
            Stack<Node<T>> stack = new Stack<>();
            Node<T> node = this.root;
            while (!stack.empty() || node != null) {
                // As long as there's a left child, follow it.
                if (node != null) {
                    stack.push(node);
                    node = node.left;
                } else {
                    // If no left child, next item is at the top of the stack; get it and follow its right child.
                    Node<T> nextNode = stack.pop();
                    consumer.accept(nextNode.item);
                    node = nextNode.right;
                }
            }
        }
        //endregion

        //region Helpers

        /**
         * Inserts an item into a subtree.
         *
         * @param item The item to insert.
         * @param node The node at the root of the current subtree.
         * @return The new root of the subtree.
         */
        private UpdateResult<T> insert(T item, Node<T> node) {
            UpdateResult<T> result = null;
            if (node == null) {
                // This is the right location for the item, but there's no node. Create one.
                result = new UpdateResult<>();
                result.node = new Node<>(item);
                this.size++;
            } else {
                T existingItem = null;
                int compareResult = this.comparator.compare(item.key(), node.item.key());
                if (compareResult < 0) {
                    // New item is smaller than the current node's item; move to the left child.
                    result = insert(item, node.left);
                    node.left = result.node;
                } else if (compareResult > 0) {
                    // New item is larger than the current node's item; move to the right child.
                    result = insert(item, node.right);
                    node.right = result.node;
                } else {
                    // Node already exists. Save the existing node's item and return it.
                    existingItem = node.item;
                }

                if (result == null) {
                    // Set the result on the current node.
                    result = new UpdateResult<>();
                    result.updatedItem = existingItem;
                }

                // Rebalance the sub-tree, if necessary.
                result.node = balance(node);
            }

            return result;
        }

        /**
         * Removes an item with given key from a subtree.
         *
         * @param key  The Key to remove.
         * @param node The node at the root of the subtree.
         * @return The new root of the subtree.
         */
        private UpdateResult<T> remove(K key, Node<T> node) {
            UpdateResult<T> result;
            if (node == null) {
                // Item not found.
                result = new UpdateResult<>();
            } else {
                int compareResult = this.comparator.compare(key, node.item.key());
                if (compareResult < 0) {
                    // Given key is smaller than the current node's item key; proceed to the node's left child.
                    result = remove(key, node.left);
                    node.left = result.node;
                } else if (compareResult > 0) {
                    // Given key is larger than the current node's item key; proceed to the node's right child.
                    result = remove(key, node.right);
                    node.right = result.node;
                } else {
                    // Found the node. Remember it's item.
                    result = new UpdateResult<>();
                    result.updatedItem = node.item;
                    if (node.left != null && node.right != null) {
                        // The node has two children. Replace the node's item with its in-order successor, and then remove
                        // that successor's node.
                        node.item = findSmallest(node.right).item;
                        node.right = remove(node.item.key(), node.right).node;
                    } else {
                        // The node has just one child. Replace it with that child.
                        node = (node.left != null) ? node.left : node.right;
                        this.size--;
                    }
                }

                // Rebalance the sub-tree, if necessary.
                result.node = balance(node);
            }

            return result;
        }

        /**
         * Rebalances the subtree with given root node, if necessary.
         *
         * @param node The root node of the subtree.
         * @return The new root node of the subtree.
         */
        private Node<T> balance(Node<T> node) {
            if (node == null) {
                return null;
            }

            int imbalance = height(node.left) - height(node.right);
            if (imbalance > MAX_IMBALANCE) {
                // Left subtree has higher height than right subtree.
                if (height(node.left.left) < height(node.left.right)) {
                    // Double rotate binary tree node: first left child with its right child; then the node with new left child.
                    node.left = rotateRight(node.left);
                }

                node = rotateLeft(node);
            } else if (-imbalance > MAX_IMBALANCE) {
                // Right subtree has higher height than left subtree.
                if (height(node.right.right) < height(node.right.left)) {
                    // Double rotate binary tree node: first right child with its left child; then the node with new right child.
                    node.right = rotateLeft(node.right);
                }

                node = rotateRight(node);
            }

            // Update current node's height.
            node.height = Math.max(height(node.left), height(node.right)) + 1;
            return node;
        }

        /**
         * Rotates the given binary tree node with the left child.
         */
        private Node<T> rotateLeft(Node<T> node) {
            Node<T> leftChild = node.left;
            node.left = leftChild.right;
            leftChild.right = node;
            node.height = Math.max(height(node.left), height(node.right)) + 1;
            leftChild.height = Math.max(height(leftChild.left), node.height) + 1;
            return leftChild;
        }

        /**
         * Rotates the given binary tree node with the right child.
         */
        private Node<T> rotateRight(Node<T> node) {
            Node<T> rightChild = node.right;
            node.right = rightChild.left;
            rightChild.left = node;
            node.height = Math.max(height(node.left), height(node.right)) + 1;
            rightChild.height = Math.max(height(rightChild.right), node.height) + 1;
            return rightChild;
        }

        /**
         * Finds the smallest item in a subtree.
         *
         * @param node The root node of the subtree.
         */
        private Node<T> findSmallest(Node<T> node) {
            if (node == null) {
                return null;
            }

            while (node.left != null) {
                node = node.left;
            }

            return node;
        }

        /**
         * Finds the largest item in a subtree.
         *
         * @param node The root node of the subtree.
         */
        private Node<T> getMax(Node<T> node) {
            if (node == null) {
                return null;
            }

            while (node.right != null) {
                node = node.right;
            }

            return node;
        }

        /**
         * Gets the current height of a node, or -1 if null.
         */
        private int height(Node<T> node) {
            return node == null ? -1 : node.height;
        }

        //endregion

        //region Helper Classes

        /**
         * Tree node.
         */
        private static class Node<T> {
            T item;
            Node<T> left;
            Node<T> right;
            int height;

            Node(T item) {
                this.item = item;
            }

            @Override
            public String toString() {
                return String.format("%s, Left = %s, Right = %s", this.item.toString(), this.left == null ? "" : this.left.item, this.right == null ? "" : this.right.item);
            }
        }

        /**
         * Intermediate result that is used to return both a node and an item.
         */
        private static class UpdateResult<T> {
            Node<T> node;
            T updatedItem;
        }

        //endregion
    }
}
