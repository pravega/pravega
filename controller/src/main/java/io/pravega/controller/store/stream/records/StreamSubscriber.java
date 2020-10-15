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

/**
 * Data class for storing information about stream's subscribers.
 */
@Data
@Slf4j
public class StreamSubscriber {
    public static final StreamSubscriberSerializer SERIALIZER = new StreamSubscriberSerializer();

    private final String subscriber;

    /**
     * Time when this record was last created/updated.
     */
    private final long updateTime;

    /**
     * Truncation Stream cut published by this subscriber
     */
    private final ImmutableMap<Long, Long> truncationStreamCut;

    @Builder
    public StreamSubscriber(@NonNull final String subscriber, @NonNull ImmutableMap<Long, Long> truncationStreamCut, final long updationTime) {
        this.subscriber = subscriber;
        this.truncationStreamCut = truncationStreamCut;
        this.updateTime = updationTime;
    }

    private static class StreamSubscriberBuilder implements ObjectBuilder<StreamSubscriber> {
    }

    @SneakyThrows(IOException.class)
    public static StreamSubscriber fromBytes(final byte[] record) {
        return SERIALIZER.deserialize(record);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }
    
    private static class StreamSubscriberSerializer
            extends VersionedSerializer.WithBuilder<StreamSubscriber, StreamSubscriberBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, StreamSubscriberBuilder recordBuilder)
                throws IOException {
            recordBuilder.subscriber(revisionDataInput.readUTF());
            recordBuilder.updationTime(revisionDataInput.readLong());
            ImmutableMap.Builder<Long, Long> streamCutBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readLong, DataInput::readLong, streamCutBuilder);
            recordBuilder.truncationStreamCut(streamCutBuilder.build());
        }

        private void write00(StreamSubscriber subscriberRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeUTF(subscriberRecord.getSubscriber());
            revisionDataOutput.writeLong(subscriberRecord.getUpdateTime());
            revisionDataOutput.writeMap(subscriberRecord.getTruncationStreamCut(), DataOutput::writeLong, DataOutput::writeLong);
        }

        @Override
        protected StreamSubscriberBuilder newBuilder() {
            return StreamSubscriber.builder();
        }
    }
}
