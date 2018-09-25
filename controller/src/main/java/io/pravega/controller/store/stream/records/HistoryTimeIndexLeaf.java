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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.pravega.common.ObjectBuilder;
import io.pravega.controller.store.stream.records.serializers.HistoryIndexLeafSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * N-ary tree's leaf node.
 *
 */
@Data
public class HistoryTimeIndexLeaf {
    public static final HistoryIndexLeafSerializer SERIALIZER = new HistoryIndexLeafSerializer();

    @Getter
    private final List<Long> records;

    @Builder
    HistoryTimeIndexLeaf(List<Long> records) {
        this.records = ImmutableList.copyOf(records);
    }

    public static class HistoryTimeIndexLeafBuilder implements ObjectBuilder<HistoryTimeIndexLeaf> {

    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @SneakyThrows(IOException.class)
    public static HistoryTimeIndexLeaf parse(final byte[] record) {
        InputStream inputStream = new ByteArrayInputStream(record, 0, record.length);
        return SERIALIZER.deserialize(inputStream);
    }

    // helper method to perform binary search
    public int findIndexAtTime(long timestamp) {
        Preconditions.checkState(!records.isEmpty());
        return RecordHelper.binarySearch(records, 0, records.size(), timestamp, x -> x);
    }

    public static HistoryTimeIndexLeaf addRecord(HistoryTimeIndexLeaf leaf, long time) {
        List<Long> records = new LinkedList<>(leaf.records);
        Long last = leaf.records.get(records.size() - 1);
        if (time > last) {
            records.add(time);
        }
        return new HistoryTimeIndexLeaf(records);
    }
}
