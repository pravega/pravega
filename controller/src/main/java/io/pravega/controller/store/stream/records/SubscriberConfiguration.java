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
import lombok.SneakyThrows;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * The configuration of a Stream Subscriber.
 */
@Data
@Builder
public class SubscriberConfiguration implements Serializable {

    public static final SubscriberConfigurationSerializer SERIALIZER = new SubscriberConfigurationSerializer();
    public static final SubscriberConfiguration EMPTY = new SubscriberConfiguration(0L, ImmutableMap.of());
    private static final long serialVersionUID = 1L;
    /**
     * Time when this configuration was updated.
     */
    private final long updateTime;

    /**
     * Truncation Stream cut published by this subscriber
     */
    private final ImmutableMap<Long, Long> streamCut;

    private static class SubscriberConfigurationBuilder implements ObjectBuilder<SubscriberConfiguration> {

    }

    @SneakyThrows(IOException.class)
    public static SubscriberConfiguration fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    static class SubscriberConfigurationSerializer
            extends VersionedSerializer.WithBuilder<SubscriberConfiguration, SubscriberConfiguration.SubscriberConfigurationBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, SubscriberConfigurationBuilder streamCutRecordBuilder)
                throws IOException {
            streamCutRecordBuilder.updateTime(revisionDataInput.readLong());
            ImmutableMap.Builder<Long, Long> streamCutBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readLong, DataInput::readLong, streamCutBuilder);
            streamCutRecordBuilder.streamCut(streamCutBuilder.build());
        }

        private void write00(SubscriberConfiguration streamCutRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(streamCutRecord.getUpdateTime());
            revisionDataOutput.writeMap(streamCutRecord.getStreamCut(), DataOutput::writeLong, DataOutput::writeLong);
        }

        @Override
        protected SubscriberConfigurationBuilder newBuilder() {
            return SubscriberConfiguration.builder();
        }
    }
}
