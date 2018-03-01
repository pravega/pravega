/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.tables;

import com.google.common.collect.Lists;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.StreamCutRecord;
import io.pravega.controller.store.stream.tables.serializers.RetentionRecordSerializerV1;
import lombok.Builder;
import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Data
@Builder
public class RetentionRecord {
    public static final VersionedSerializer.WithBuilder<RetentionRecord, RetentionRecord.RetentionRecordBuilder> SERIALIZER_V1 =
            new RetentionRecordSerializerV1();

    private final List<StreamCutRecord> streamCuts;

    public RetentionRecord(List<StreamCutRecord> streamCuts) {
        this.streamCuts = Collections.unmodifiableList(streamCuts);
    }

    public static RetentionRecord addStreamCutIfLatest(RetentionRecord record, StreamCutRecord cut) {
        List<StreamCutRecord> list = Lists.newArrayList(record.streamCuts);

        // add only if cut.recordingTime is newer than any previous cut
        if (list.stream().noneMatch(x -> x.getRecordingTime() >= cut.getRecordingTime())) {
            list.add(cut);
        }

        return new RetentionRecord(list);
    }

    public static RetentionRecord removeStreamCutBefore(RetentionRecord record, StreamCutRecord cut) {
        List<StreamCutRecord> list = Lists.newArrayList(record.streamCuts);

        // remove all stream cuts with recordingTime before supplied cut
        return new RetentionRecord(list.stream().filter(x -> x.getRecordingTime() > cut.getRecordingTime())
                .collect(Collectors.toList()));
    }

    public static class RetentionRecordBuilder implements ObjectBuilder<RetentionRecord> {
    }
}
