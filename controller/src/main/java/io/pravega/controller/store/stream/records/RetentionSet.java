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
import com.google.common.collect.Lists;
import io.pravega.common.ObjectBuilder;
import io.pravega.controller.store.stream.records.serializers.RetentionSetSerializer;
import io.pravega.controller.store.stream.tables.StreamCutRecord;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Builder
public class RetentionSet {
    public static final RetentionSetSerializer SERIALIZER = new RetentionSetSerializer();

    @Getter
    private final List<RetentionSetRecord> retentionRecords;

    public RetentionSet(List<RetentionSetRecord> retentionSetRecords) {
        this.retentionRecords = ImmutableList.copyOf(retentionSetRecords);
    }

    public static RetentionSet addStreamCutIfLatest(RetentionSet record, StreamCutRecord cut) {
        List<RetentionSetRecord> list = Lists.newArrayList(record.retentionRecords);

        // add only if cut.recordingTime is newer than any previous cut
        if (list.stream().noneMatch(x -> x.getRecordingTime() >= cut.getRecordingTime())) {
            list.add(new RetentionSetRecord(cut.getRecordingTime(), cut.getRecordingSize()));
        }

        return new RetentionSet(list);
    }

    public List<RetentionSetRecord> retentionRecordsBefore(RetentionSetRecord record) {
        return retentionRecords.stream().filter(x -> x.getRecordingTime() < record.getRecordingTime())
                .collect(Collectors.toList());
    }

    public static RetentionSet removeStreamCutBefore(RetentionSet set, RetentionSetRecord record) {
        // remove all stream cuts with recordingTime before supplied cut
        return new RetentionSet(set.retentionRecords.stream().filter(x -> x.getRecordingTime() > record.getRecordingTime())
                .collect(Collectors.toList()));
    }

    public RetentionSetRecord getLatest() {
        return retentionRecords.get(retentionRecords.size() - 1);
    }

    public static class RetentionSetBuilder implements ObjectBuilder<RetentionSet> {
    }

    @SneakyThrows(IOException.class)
    public static RetentionSet parse(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }
}
