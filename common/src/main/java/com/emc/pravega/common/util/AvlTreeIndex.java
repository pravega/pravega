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
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Stack;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.val;

/**
 * SortedIndex backed by an AVL Tree.
 * <p>
 * Note: This class is not thread-safe and requires external synchronization when in a multi-threaded environment.
 *
 * @param <K> The type of the Key.
 * @param <V> The type of the IndexEntries.
 */
@NotThreadSafe
public class AvlTreeIndex<K, V extends SortedIndex.IndexEntry<K>> implements SortedIndex<K, V> {
    //region Members

    private static final int MAX_IMBALANCE = 1;
    private final Comparator<K> comparator;
    private transient Node<V> root;
    private transient int size;
    private transient int modCount;
    private transient V first;
    private transient V last;
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
        this.first = null;
        this.last = null;
    }

    //endregion

    //region SortedIndex Implementation

    @Override
    public void clear() {
        this.root = null;
        this.first = null;
        this.last = null;
        this.size = 0;
        this.modCount++;
    }

    @Override
    public V put(V item) {
        Preconditions.checkNotNull(item, "item");
        Preconditions.checkNotNull(item.key(), "item.key()");

        val result = insert(item, this.root);
        this.root = result.node;
        if (this.size == 1) {
            // Only one item in the index; this is the highest and lowest at the same time.
            this.last = item;
            this.first = item;
        } else {
            if (this.first != null && this.comparator.compare(this.first.key(), item.key()) > 0) {
                // The freshly inserted item is smaller than the previous smallest item, so update it.
                this.first = item;
            }

            if (this.last != null && this.comparator.compare(this.last.key(), item.key()) < 0) {
                // The freshly inserted item is larger than the previous largest item, so update it.
                this.last = item;
            }
        }

        return result.updatedItem;
    }

    @Override
    public V remove(K key) {
        Preconditions.checkNotNull(key, "key");

        val result = delete(key, this.root);
        this.root = result.node;
        if (result.updatedItem != null) {
            if (this.first != null && this.comparator.compare(key, this.first.key()) >= 0) {
                // We have removed the smallest item; clear its cached value.
                this.first = null;
            }

            if (this.last != null && this.comparator.compare(key, this.last.key()) <= 0) {
                // We have removed the largest item; clear its cached value.
                this.last = null;
            }
        }

        return result.updatedItem;
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public V get(K key) {
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

        return null;
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

        // Return the result, whether we found something or not.
        return result;
    }

    @Override
    public V getFloor(K key) {
        // No need to check key != null, since we do not risk corrupting the tree with input is not valid.
        Node<V> node = this.root;
        Node<V> lastRightChildParent = null;
        V result = null;
        while (node != null && result == null) {
            // Compare the key to the current node's item.
            int compareResult = this.comparator.compare(key, node.item.key());
            if (compareResult > 0) {
                // Sought key is larger than the current node's item.
                if (node.right == null) {
                    // No more nodes to the right, so this is the largest item with a key smaller than the given one.
                    result = node.item;
                } else {
                    // The right child has a key smaller than this one; Search again there.
                    lastRightChildParent = node;
                    node = node.right;
                }
            } else if (compareResult < 0) {
                // Sought key is smaller than the current node's item.
                if (node.left != null) {
                    // Search again to the left (where we'll have a smaller key).
                    node = node.left;
                } else if (lastRightChildParent != null) {
                    // There are no more nodes to the left - this is the node with the smallest item in the Tree.
                    // If we had a pointer back to the parent, we would have to walk up the tree path as long as we
                    // have a parent and the current node is a left child, then return the parent of that node.
                    // In other words, if there exists a result, it is the parent of the last node that was selected as a
                    // right child, which is conveniently stored in lastRightChildParent.
                    result = lastRightChildParent.item;
                } else {
                    // Nothing could be found.
                    return null;
                }
            } else {
                // Exact match; return it.
                result = node.item;
            }
        }

        // Return the result, whether we found something or not.
        return result;
    }

    @Override
    public V getFirst() {
        if (this.size == 0) {
            return null;
        } else if (this.first != null) {
            return this.first;
        } else {
            this.first = findSmallest(this.root).item;
            return this.first;
        }
    }

    @Override
    public V getLast() {
        if (this.size == 0) {
            return null;
        } else if (this.last != null) {
            return this.last;
        } else {
            this.last = findLargest(this.root).item;
            return this.last;
        }
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

        int imbalance = getHeight(node.left) - getHeight(node.right);
        if (imbalance > MAX_IMBALANCE) {
            // Left subtree has higher height than right subtree.
            if (getHeight(node.left.left) < getHeight(node.left.right)) {
                // Double rotate binary tree node: first left child with its right child; then the node with new left child.
                node.left = rotateRight(node.left);
            }

            node = rotateLeft(node);
        } else if (-imbalance > MAX_IMBALANCE) {
            // Right subtree has higher height than left subtree.
            if (getHeight(node.right.right) < getHeight(node.right.left)) {
                // Double rotate binary tree node: first right child with its left child; then the node with new right child.
                node.right = rotateLeft(node.right);
            }

            node = rotateRight(node);
        } else {
            // No rotation needed, just update current node's height, as an update may have changed it.
            node.height = calculateHeight(getHeight(node.left), getHeight(node.right));
        }

        return node;
    }

    /**
     * Rotates the given binary tree node with the left child. At the end of this operation:
     * * The Left Child's right node is the Given Node
     * * The Given Node's Left Child is the Left Child's original Right Child.
     */
    private Node<V> rotateLeft(Node<V> node) {
        Node<V> leftChild = node.left;
        node.left = leftChild.right;
        leftChild.right = node;
        node.height = calculateHeight(getHeight(node.left), getHeight(node.right));
        leftChild.height = calculateHeight(getHeight(leftChild.left), node.height);
        return leftChild;
    }

    /**
     * Rotates the given binary tree node with the right child. At the end of this operation:
     * * The Right Child's left node is the Given Node
     * * The Given Node's Right child is the Right Child's original Left Child.
     */
    private Node<V> rotateRight(Node<V> node) {
        Node<V> rightChild = node.right;
        node.right = rightChild.left;
        rightChild.left = node;
        node.height = calculateHeight(getHeight(node.left), getHeight(node.right));
        rightChild.height = calculateHeight(getHeight(rightChild.right), node.height);
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
    private byte getHeight(Node<V> node) {
        return node == null ? -1 : node.height;
    }

    private byte calculateHeight(byte leftHeight, byte rightHeight) {
        return (byte) ((leftHeight >= rightHeight ? leftHeight : rightHeight) + 1);
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
        byte height;

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
