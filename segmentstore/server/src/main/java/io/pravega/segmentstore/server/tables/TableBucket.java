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
 * <
 * This object is constructed with every access call to the TableService and is a function of the input Key and existing
 * Table State. As such, it may or may not contain a full path to the Bucket itself.
 *
 * If this list is empty or partial (getLastNode().isIndexNode() == true), then this does not lead to a bucket itself, and
 * means the following:
 * - During a read: the sought Key does not exist
 * - During a write: the sought Key does not collide with any other key.
 *
 * If this list is complete (getLastNode() != null && getLastNode.isIndexNode() == false), then this points to a real bucket,
 * and means the following:
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
     * An ordered list of all Nodes that lead to this bucket.
     */
    @Getter
    private final List<Node> nodes;

    Node getLastNode() {
        return this.nodes.isEmpty() ? null : this.nodes.get(this.nodes.size() - 1);
    }

    @Override
    public String toString() {
        return String.format("Nodes = %s, LastNode = {%s}", this.nodes.size(), getLastNode());
    }

    public static class TableBucketBuilder {
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

        Node(boolean indexNode, @NonNull UUID key, long value) {
            this.indexNode = indexNode;
            this.key = key;
            this.value = value;
            Preconditions.checkArgument(!this.indexNode || value <= Integer.MAX_VALUE, "Invalid value for index node.");
        }

        @Override
        public String toString() {
            return String.format("Key={%s}, Value=%s, Index=%s", this.key, this.value, this.indexNode);
        }
    }
}