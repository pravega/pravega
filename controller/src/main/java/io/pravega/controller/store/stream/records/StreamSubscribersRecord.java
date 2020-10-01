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

import com.google.common.base.Preconditions;
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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Data class for storing information about stream's subscribers.
 */
@Data
@Slf4j
public class StreamSubscribersRecord {
    public static final SubscribersRecordSerializer SERIALIZER = new SubscribersRecordSerializer();
    public static final StreamSubscribersRecord EMPTY = new StreamSubscribersRecord(ImmutableMap.of());

    private final ImmutableMap<String, SubscriberConfiguration> streamSubscribers;

    @Builder
    public StreamSubscribersRecord(@NonNull ImmutableMap<String, SubscriberConfiguration> streamSubcribers) {
        this.streamSubscribers = streamSubcribers;
    }

    public boolean contains(String subscriber) {
        Preconditions.checkArgument(streamSubscribers != null, "Null subscribers for Stream");
        return streamSubscribers.containsKey(subscriber);
    }

    public static StreamSubscribersRecord update(ImmutableMap<String, SubscriberConfiguration> existingSubscribers,
                                                 String newSubscriber, SubscriberConfiguration config) {
        Map<String, SubscriberConfiguration> streamSubscribers = new HashMap<String, SubscriberConfiguration>();
        streamSubscribers.putAll(existingSubscribers);
        streamSubscribers.put(newSubscriber, config);
        return new StreamSubscribersRecord(ImmutableMap.copyOf(streamSubscribers));
    }

    public static StreamSubscribersRecord remove(ImmutableMap<String, SubscriberConfiguration> existingSubscribers,
                                                 String subscriber) {
        Map<String, SubscriberConfiguration> streamSubscribers = existingSubscribers.entrySet().stream()
                                                                .filter(e -> !e.getKey().equals(subscriber))
                                                                .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
        return new StreamSubscribersRecord(ImmutableMap.copyOf(streamSubscribers));
    }

    private static class StreamSubscribersRecordBuilder implements ObjectBuilder<StreamSubscribersRecord> {
    }

    @SneakyThrows(IOException.class)
    public static StreamSubscribersRecord fromBytes(final byte[] record) {
        //InputStream inputStream = new ByteArrayInputStream(record, 0, record.length);
        return SERIALIZER.deserialize(record);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }
    
    private static class SubscribersRecordSerializer
            extends VersionedSerializer.WithBuilder<StreamSubscribersRecord, StreamSubscribersRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput,
                            StreamSubscribersRecordBuilder recordBuilder)
                throws IOException {
            ImmutableMap.Builder<String, SubscriberConfiguration> mapBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readUTF, SubscriberConfiguration.SERIALIZER::deserialize, mapBuilder);
            recordBuilder.streamSubcribers(mapBuilder.build());
        }

        private void write00(StreamSubscribersRecord streamSubscribersRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeMap(streamSubscribersRecord.getStreamSubscribers(),
                                         DataOutput::writeUTF,
                                         SubscriberConfiguration.SERIALIZER::serialize);

        }

        @Override
        protected StreamSubscribersRecordBuilder newBuilder() {
            return StreamSubscribersRecord.builder();
        }
    }
}
