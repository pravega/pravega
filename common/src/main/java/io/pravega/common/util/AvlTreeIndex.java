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
import java.util.ConcurrentModificationException;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.val;

/**
 * SortedIndex backed by an AVL Tree.
 * <p>
 * Note: This class is not thread-safe and requires external synchronization when in a multi-threaded environment.
 *
 * @param <V> The type of the IndexEntries.
 */
@NotThreadSafe
public class AvlTreeIndex<V extends SortedIndex.IndexEntry> implements SortedIndex<V> {
    //region Members

    private static final int MAX_IMBALANCE = 1;
    private transient Node root;
    private transient int size;
    private transient int modCount;
    private transient V first;
    private transient V last;
    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AvlTreeIndex class.
     */
    public AvlTreeIndex() {
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

        val result = insert(item, this.root);
        this.root = result.node;

        if (this.size == 1) {
            // Only one item in the index; this is the highest and lowest at the same time.
            this.last = item;
            this.first = item;
        } else {
            if (this.first != null && (this.first.key() >= item.key())) {
                // The freshly inserted item is smaller than the previous smallest item, so update it.
                this.first = item;
            }

            if (this.last != null && (this.last.key() <= item.key())) {
                // The freshly inserted item is larger than the previous largest item, so update it.
                this.last = item;
            }
        }

        return result.updatedItem;
    }

    @Override
    public V remove(long key) {
        val result = delete(key, this.root);
        this.root = result.node;
        if (result.updatedItem != null) {
            if (this.first != null && (key <= this.first.key()) || this.size == 0) {
                // We have removed the smallest item; clear its cached value.
                this.first = null;
            }

            if (this.last != null && (key >= this.last.key()) || this.size == 0) {
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
    public V get(long key) {
        Node node = this.root;
        while (node != null) {
            long itemKey = node.item.key();
            if (key < itemKey) {
                node = node.left;
            } else if (key > itemKey) {
                node = node.right;
            } else {
                return node.item;
            }
        }

        return null;
    }

    @Override
    public V getCeiling(long key) {
        Node node = this.root;
        Node lastLeftChildParent = null;
        V result = null;
        while (node != null && result == null) {
            // Compare the key to the current node's item.
            long itemKey = node.item.key();
            if (key < itemKey) {
                // Sought key is smaller than the current node's item.
                if (node.left == null) {
                    // No more nodes to the left, so this is the smallest item with a key greater than the given one.
                    result = node.item;
                } else {
                    // The left child has a key smaller than this one; Search again there.
                    lastLeftChildParent = node;
                    node = node.left;
                }
            } else if (key > itemKey) {
                // Sought key is greater than the current node's item key; search again to the right (if we can).
                node = node.right;
                if (node == null && lastLeftChildParent != null) {
                    // There are no more nodes to the right - this is the node with the largest item in the Tree.
                    // If we had a pointer back to the parent, we would have to walk up the tree path as long as we
                    // have a parent and the current node is a right child, then return the parent of that node.
                    // In other words, if there exists a result, it is the parent of the last node that was selected as a
                    // left child, which is conveniently stored in lastLeftChildParent.
                    result = lastLeftChildParent.item;
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
    public V getFloor(long key) {
        Node node = this.root;
        Node lastRightChildParent = null;
        V result = null;
        while (node != null && result == null) {
            // Compare the key to the current node's item.
            long itemKey = node.item.key();
            if (key > itemKey) {
                // Sought key is larger than the current node's item.
                if (node.right == null) {
                    // No more nodes to the right, so this is the largest item with a key smaller than the given one.
                    result = node.item;
                } else {
                    // The right child has a key smaller than this one; Search again there.
                    lastRightChildParent = node;
                    node = node.right;
                }
            } else if (key < itemKey) {
                // Sought key is smaller than the current node's item key; search again to the left (if we can).
                node = node.left;
                if (node == null && lastRightChildParent != null) {
                    // There are no more nodes to the left - this is the node with the smallest item in the Tree.
                    // If we had a pointer back to the parent, we would have to walk up the tree path as long as we
                    // have a parent and the current node is a left child, then return the parent of that node.
                    // In other words, if there exists a result, it is the parent of the last node that was selected as a
                    // right child, which is conveniently stored in lastRightChildParent.
                    result = lastRightChildParent.item;
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
        if (this.first == null) {
            this.first = findSmallest(this.root);
        }

        return this.first;
    }

    @Override
    public V getLast() {
        if (this.last == null) {
            this.last = findLargest(this.root);
        }

        return this.last;
    }

    @Override
    public void forEach(Consumer<V> consumer) {
        Preconditions.checkNotNull(consumer, "consumer");
        TraversalStack stack = new TraversalStack(getHeight(this.root) + 1);
        Node node = this.root;
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
                Node nextNode = stack.pop();
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
    private UpdateResult insert(V item, Node node) {
        UpdateResult result;
        if (node == null) {
            // This is the right location for the item, but there's no node. Create one.
            result = new UpdateResult();
            result.node = new Node(item);
            this.size++;
            this.modCount++;
        } else {
            long itemKey = item.key();
            long nodeKey = node.item.key();
            if (itemKey < nodeKey) {
                // New item is smaller than the current node's item; move to the left child.
                result = insert(item, node.left);
                node.left = result.node;
            } else if (itemKey > nodeKey) {
                // New item is larger than the current node's item; move to the right child.
                result = insert(item, node.right);
                node.right = result.node;
            } else {
                // Node already exists. Save the existing node's item and return it.
                result = new UpdateResult();
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
    private UpdateResult delete(long key, Node node) {
        UpdateResult result;
        if (node == null) {
            // Item not found.
            result = new UpdateResult();
        } else {
            long itemKey = node.item.key();
            if (key < itemKey) {
                // Given key is smaller than the current node's item key; proceed to the node's left child.
                result = delete(key, node.left);
                node.left = result.node;
            } else if (key > itemKey) {
                // Given key is larger than the current node's item key; proceed to the node's right child.
                result = delete(key, node.right);
                node.right = result.node;
            } else {
                // Found the node. Remember it's item.
                result = new UpdateResult();
                result.updatedItem = node.item;
                if (node.left != null && node.right != null) {
                    // The node has two children. Replace the node's item with its in-order successor, and then remove
                    // that successor's node.
                    node.item = findSmallest(node.right);
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
    private Node balance(Node node) {
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

            return rotateLeft(node);
        } else if (-imbalance > MAX_IMBALANCE) {
            // Right subtree has higher height than left subtree.
            if (getHeight(node.right.right) < getHeight(node.right.left)) {
                // Double rotate binary tree node: first right child with its left child; then the node with new right child.
                node.right = rotateLeft(node.right);
            }

            return rotateRight(node);
        } else {
            // No rotation needed, just update current node's height, as an update may have changed it.
            node.height = calculateHeight(getHeight(node.left), getHeight(node.right));
            return node;
        }
    }

    /**
     * Rotates the given binary tree node with the left child. At the end of this operation:
     * * The Left Child's right node is the Given Node
     * * The Given Node's Left Child is the Left Child's original Right Child.
     */
    private Node rotateLeft(Node node) {
        Node leftChild = node.left;
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
    private Node rotateRight(Node node) {
        Node rightChild = node.right;
        node.right = rightChild.left;
        rightChild.left = node;
        node.height = calculateHeight(getHeight(node.left), getHeight(node.right));
        rightChild.height = calculateHeight(getHeight(rightChild.right), node.height);
        return rightChild;
    }

    /**
     * Gets the current height of a node, or -1 if null.
     */
    private byte getHeight(Node node) {
        return node == null ? -1 : node.height;
    }

    private byte calculateHeight(byte leftHeight, byte rightHeight) {
        return (byte) ((leftHeight >= rightHeight ? leftHeight : rightHeight) + 1);
    }

    /**
     * Finds the smallest item in a subtree.
     *
     * @param node The root node of the subtree.
     */
    private V findSmallest(Node node) {
        if (node == null) {
            return null;
        }

        while (node.left != null) {
            node = node.left;
        }

        return node.item;
    }

    /**
     * Finds the largest item in a subtree.
     *
     * @param node The root node of the subtree.
     */
    private V findLargest(Node node) {
        if (node == null) {
            return null;
        }

        while (node.right != null) {
            node = node.right;
        }

        return node.item;
    }

    //endregion

    //region Helper Classes

    /**
     * Tree node.
     */
    private class Node {
        V item;
        Node left;
        Node right;
        byte height;

        Node(V item) {
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
    private class UpdateResult {
        Node node;
        V updatedItem;
    }

    /**
     * Array-backed fixed-size stack. This is faster than using java.util.Stack due to less overhead inherited from the Vector class.
     */
    private class TraversalStack {
        private final Object[] nodes;
        private int size;

        TraversalStack(int maxSize) {
            this.nodes = new Object[maxSize];
            this.size = 0;
        }

        void push(Node node) {
            this.nodes[this.size++] = node;
        }

        @SuppressWarnings("unchecked")
        Node pop() {
            return (Node) this.nodes[--this.size];
        }

        boolean empty() {
            return this.size == 0;
        }
    }

    //endregion
}
