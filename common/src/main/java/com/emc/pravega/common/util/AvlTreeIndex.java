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
import lombok.val;

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Stack;
import java.util.function.Consumer;

/**
 * SortedIndex backed by an AVL Tree.
 *
 * @param <K> The type of the Key.
 * @param <V> The type of the IndexEntries.
 */
public class AvlTreeIndex<K, V extends IndexEntry<K>> implements SortedIndex<K, V> {
    //region Members

    private static final int MAX_IMBALANCE = 1;
    private final Comparator<K> comparator;
    private Node<V> root;
    private int size;
    private int modCount;
    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AvlTreeIndex class.
     *
     * @param comparator A Key comparator to use as orderer within the index.
     */
    public AvlTreeIndex(Comparator<K> comparator) {
        Preconditions.checkNotNull(comparator, "comparator");
        this.comparator = comparator;
        this.root = null;
        this.size = 0;
        this.modCount = 0;
    }

    //endregion

    //region SortedIndex Implementation

    @Override
    public void clear() {
        this.root = null;
        this.size = 0;
        this.modCount++;
    }

    @Override
    public V put(V item) {
        Preconditions.checkNotNull(item, "item");
        Preconditions.checkNotNull(item.key(), "item.key()");

        val result = insert(item, this.root);
        this.root = result.node;
        return result.updatedItem;
    }

    @Override
    public V remove(K key) {
        Preconditions.checkNotNull(key, "key");

        val result = delete(key, this.root);
        this.root = result.node;
        return result.updatedItem;
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public V get(K key) {
        return get(key, null);
    }

    @Override
    public V get(K key, V defaultValue) {
        // No need to check key != null, since we do not risk corrupting the tree with input is not valid.
        Node<V> node = this.root;
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
    public V getCeiling(K key) {
        // No need to check key != null, since we do not risk corrupting the tree with input is not valid.
        Node<V> node = this.root;
        Node<V> lastLeftChildParent = null;
        V result = null;
        while (node != null && result == null) {
            // Compare the key to the current node's item.
            int compareResult = this.comparator.compare(key, node.item.key());
            if (compareResult < 0) {
                // Sought key is smaller than the current node's item.
                if (node.left == null) {
                    // No more nodes to the left, so this is the smallest item with a key greater than the given one.
                    //return node.item;
                    result = node.item;
                } else {
                    // The left child has a key smaller than this one; Search again there.
                    lastLeftChildParent = node;
                    node = node.left;
                }
            } else if (compareResult > 0) {
                // Sought key is greater than the current node's item.
                if (node.right != null) {
                    // Search again to the right (where we'll have a larger key).
                    node = node.right;
                } else if (lastLeftChildParent != null) {
                    // There are no more nodes to the right - this is the node with the largest item in the Tree.
                    // If we had a pointer back to the parent, we would have to walk up the tree path as long as we
                    // have a parent and the current node is a right child, then return the parent of that node.
                    // In other words, if there exists a result, it is the parent of the last node that was selected as a
                    // left child, which is conveniently stored in lastLeftChildParent.
                    result = lastLeftChildParent.item;
                } else {
                    // Nothing could be found.
                    return null;
                }
            } else {
                // Exact match; return it.
                result = node.item;
            }
        }

        // Nothing could be found.
        return result;
    }

    @Override
    public V getFirst() {
        if (this.size == 0) {
            return null;
        }

        return findSmallest(this.root).item;
    }

    @Override
    public V getLast() {
        if (this.size == 0) {
            return null;
        }

        return findLargest(this.root).item;
    }

    @Override
    public void forEach(Consumer<V> consumer) {
        Preconditions.checkNotNull(consumer, "consumer");
        Stack<Node<V>> stack = new Stack<>();
        Node<V> node = this.root;
        final int originalModCount = this.modCount;
        while (!stack.empty() || node != null) {
            if (originalModCount != this.modCount) {
                throw new ConcurrentModificationException("AvlTreeIndex has been modified; forEach cannot continue.");
            }

            // As long as there's a left child, follow it.
            if (node != null) {
                stack.push(node);
                node = node.left;
            } else {
                // If no left child, next item is at the top of the stack; get it and follow its right child.
                Node<V> nextNode = stack.pop();
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
    private UpdateResult<V> insert(V item, Node<V> node) {
        UpdateResult<V> result;
        if (node == null) {
            // This is the right location for the item, but there's no node. Create one.
            result = new UpdateResult<>();
            result.node = new Node<>(item);
            this.size++;
            this.modCount++;
        } else {
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
                result = new UpdateResult<>();
                result.updatedItem = node.item;
                node.item = item;
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
    private UpdateResult<V> delete(K key, Node<V> node) {
        UpdateResult<V> result;
        if (node == null) {
            // Item not found.
            result = new UpdateResult<>();
        } else {
            int compareResult = this.comparator.compare(key, node.item.key());
            if (compareResult < 0) {
                // Given key is smaller than the current node's item key; proceed to the node's left child.
                result = delete(key, node.left);
                node.left = result.node;
            } else if (compareResult > 0) {
                // Given key is larger than the current node's item key; proceed to the node's right child.
                result = delete(key, node.right);
                node.right = result.node;
            } else {
                // Found the node. Remember it's item.
                result = new UpdateResult<>();
                result.updatedItem = node.item;
                if (node.left != null && node.right != null) {
                    // The node has two children. Replace the node's item with its in-order successor, and then remove
                    // that successor's node.
                    node.item = findSmallest(node.right).item;
                    node.right = delete(node.item.key(), node.right).node;
                } else {
                    // The node has just one child. Replace it with that child.
                    node = (node.left != null) ? node.left : node.right;
                    this.size--;
                    this.modCount++;
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
    private Node<V> balance(Node<V> node) {
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
    private Node<V> rotateLeft(Node<V> node) {
        Node<V> leftChild = node.left;
        node.left = leftChild.right;
        leftChild.right = node;
        node.height = Math.max(height(node.left), height(node.right)) + 1;
        leftChild.height = Math.max(height(leftChild.left), node.height) + 1;
        return leftChild;
    }

    /**
     * Rotates the given binary tree node with the right child.
     */
    private Node<V> rotateRight(Node<V> node) {
        Node<V> rightChild = node.right;
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
    private Node<V> findSmallest(Node<V> node) {
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
    private Node<V> findLargest(Node<V> node) {
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
    private int height(Node<V> node) {
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
