/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * Metadata about a Table Bucket.
 *
 * Every instance of this object is a function of a Key and existing Table State. As such, it may or may not contain a
 * full path to the Table Bucket itself (for example, if an existing path points to another Key, however the Key used
 * to generate this only partially shares that path but is not otherwise in the table, then we'll have a partial path).
 *
 * If the node path is complete (getLastNode() != null && getLastNode.isIndexNode() == false), then this points to a real
 * bucket, and means the following:
 * - getLastNode().getValue() points to an Offset within the Segment where the latest value for any Key in this Bucket is
 * written.
 * - During a read: the sought value is either at the given offset or will need to be located by means of backpointers (not stored here).
 * - During a write: there already exists another value for a Key in this bucket (the Key may or may not be the same as
 * the one that generated this object).
 */
@Builder
class TableBucket {
    static final long NO_NODE = -1;
    /**
     * An ordered list of all Nodes (the path) that lead to this bucket.
     */
    @Getter
    private final List<Node> nodes;

    /**
     * Gets the last {@link Node} in the node path, or null if the path is empty.
     */
    Node getLastNode() {
        return this.nodes.isEmpty() ? null : this.nodes.get(this.nodes.size() - 1);
    }

    /**
     * Gets a value indicating whether this bucket is incomplete. A bucket is incomplete if it doesn't point to a data
     * node (i.e., no node path or partial node path).
     *
     * @return True if partial, false if it points to a data node.
     */
    boolean isPartial() {
        return this.nodes.isEmpty() || this.nodes.get(this.nodes.size() - 1).isIndexNode();
    }

    @Override
    public int hashCode() {
        Node last = getLastNode();
        return last == null ? 0 : Long.hashCode(last.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TableBucket) {
            // Each TableBucket is defined by the last node in its node path. If the last node is a data node,
            // then that will contain a pointer to a segment offset (which uniquely defines the bucket). Otherwise the
            // combination of Node.key and Node.value also uniquely defines this bucket.
            TableBucket other = (TableBucket) obj;
            return this.nodes.size() == other.nodes.size()
                    && Node.equals(this.getLastNode(), other.getLastNode());
        }

        return false;
    }

    @Override
    public String toString() {
        return String.format("Nodes = %s, LastNode = {%s}", this.nodes.size(), getLastNode());
    }

    //region Node

    /**
     * Metadata about a Node in a Bucket.
     */
    @Getter
    static class Node {
        /**
         * True if this is an index node (getValue() points to another Node).
         * False if a data node (getValue() points to an offset within a Segment.
         */
        private final boolean indexNode;
        /**
         * Segment Attribute Key pertaining to this node.
         */
        private final UUID key;
        /**
         * Segment Attribute value pertaining to this node. This is either a pointer to another node or a pointer to a
         * Segment location.
         */
        private final long value;

        /**
         * Creates a new instance of the Node class.
         *
         * @param indexNode True if this represents an Index node, false otherwise.
         * @param key       A {@link UUID} representing the Key of this Node.
         * @param value     The value stored in this node.
         * @throws IllegalArgumentException If indexNodes == true and value exceeds {@link Integer#MAX_VALUE}.
         */
        Node(boolean indexNode, @NonNull UUID key, long value) {
            this.indexNode = indexNode;
            this.key = key;
            this.value = value;
            Preconditions.checkArgument(!this.indexNode || value <= Integer.MAX_VALUE, "Invalid value for index node.");
        }

        static boolean equals(Node n1, Node n2) {
            return n1 == null && n2 == null
                    || (n1 != null && n2 != null && n1.key.equals(n2.key) && n1.value == n2.value);

        }

        @Override
        public String toString() {
            return String.format("Key={%s}, Value=%s, Index=%s", this.key, this.value, this.indexNode);
        }
    }

    //endregion

    //region Builder

    static class TableBucketBuilder {
        @Getter
        private Node lastNode;

        TableBucketBuilder() {
            this.nodes = new ArrayList<>();
        }

        TableBucketBuilder node(Node node) {
            this.lastNode = node;
            if (node != null) {
                this.nodes.add(node);
            }

            return this;
        }
    }

    //endregion
}