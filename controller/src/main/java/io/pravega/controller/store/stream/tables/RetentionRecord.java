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
import io.pravega.controller.store.stream.StreamCutRecord;
import lombok.Data;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class RetentionRecord implements Serializable {
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

        // remove all stream cut with recordingTime before supplied cut
        return new RetentionRecord(list.stream().filter(x -> x.getRecordingTime() > cut.getRecordingTime())
                .collect(Collectors.toList()));
    }
}
