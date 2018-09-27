/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import com.google.common.collect.ImmutableList;
import io.pravega.common.ObjectBuilder;
import io.pravega.controller.store.stream.records.serializers.HistoryIndexRootNodeSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

@Data
/**
 * Root node for an n-ary tree for storing time index of epoch records.
 * This contains a reference to leaf nodes. The reference is based on the timestamp of first record in the leaf node.
 */
public class HistoryTimeIndexRootNode {
    public static final HistoryIndexRootNodeSerializer SERIALIZER = new HistoryIndexRootNodeSerializer();

    // Sorted leaves
    private final List<Long> leaves;

    @Builder
    HistoryTimeIndexRootNode(List<Long> leaves) {
        this.leaves = ImmutableList.copyOf(leaves);
    }

    public static class HistoryTimeIndexRootNodeBuilder implements ObjectBuilder<HistoryTimeIndexRootNode> {

    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @SneakyThrows(IOException.class)
    public static HistoryTimeIndexRootNode parse(final byte[] record) {
        InputStream inputStream = new ByteArrayInputStream(record, 0, record.length);
        return SERIALIZER.deserialize(inputStream);
    }

    // helper method to perform binary search and find the appropriate leaf node which may have the history record.

    // helper method to add a new leaf node record here
    public int findLeafNode(long time) {
        // binary search time to find the index which corresponds to leaf node
        return RecordHelper.binarySearch(leaves, time, x -> x);
    }

    public static HistoryTimeIndexRootNode addNewLeaf(HistoryTimeIndexRootNode rootNode, long time) {
        LinkedList<Long> leaves = new LinkedList<>(rootNode.leaves);
        if (time > rootNode.leaves.get(rootNode.leaves.size() - 1)) {
            leaves.add(time);
        }
        return new HistoryTimeIndexRootNode(leaves);
    }
}
