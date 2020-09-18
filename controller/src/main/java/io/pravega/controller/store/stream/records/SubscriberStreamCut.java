/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * This is data class for storing Subscriber Stream cut with time when the cut was submitted.
 */
@Data
@Slf4j
public class SubscriberStreamCut {
    public static final SubscriberStreamCutSerializer SERIALIZER = new SubscriberStreamCutSerializer();
    public static final SubscriberStreamCut EMPTY = new SubscriberStreamCut(0L, ImmutableMap.of());
    /**
     * Time when this stream cut was updated.
     */
    final long updateTime;
    /**
     * Stream cut for truncation
     */
    final ImmutableMap<Long, Long> streamCut;

    @Builder
    public SubscriberStreamCut(long updateTime, @NonNull ImmutableMap<Long, Long> streamCut) {
        this.updateTime = updateTime;
        this.streamCut = streamCut;
    }

    public Map<Long, Long> getStreamCut() {
        return Collections.unmodifiableMap(streamCut);
    }

    private static class SubscriberStreamCutBuilder implements ObjectBuilder<SubscriberStreamCut> {

    }

    @SneakyThrows(IOException.class)
    public static SubscriberStreamCut fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    static class SubscriberStreamCutSerializer
            extends VersionedSerializer.WithBuilder<SubscriberStreamCut, SubscriberStreamCut.SubscriberStreamCutBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }
        
        private void read00(RevisionDataInput revisionDataInput, SubscriberStreamCutBuilder streamCutRecordBuilder)
                throws IOException {
            streamCutRecordBuilder.updateTime(revisionDataInput.readLong());
            ImmutableMap.Builder<Long, Long> streamCutBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readLong, DataInput::readLong, streamCutBuilder);
            streamCutRecordBuilder.streamCut(streamCutBuilder.build());
        }

        private void write00(SubscriberStreamCut streamCutRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(streamCutRecord.getUpdateTime());
            revisionDataOutput.writeMap(streamCutRecord.getStreamCut(), DataOutput::writeLong, DataOutput::writeLong);
        }

        @Override
        protected SubscriberStreamCutBuilder newBuilder() {
            return SubscriberStreamCut.builder();
        }
    }

}
