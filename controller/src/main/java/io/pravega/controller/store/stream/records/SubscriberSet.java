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

import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Data class to capture a subscriber set.
 */
@Slf4j
@Data
public class SubscriberSet {
    public static final SubscriberSetSerializer SERIALIZER = new SubscriberSetSerializer();

    @Getter
    private final ImmutableMap<String, Long> subscribers;

    @Builder
    public SubscriberSet(@NonNull ImmutableMap<String, Long> subscribers) {
        this.subscribers = subscribers;
    }

    /**
     * This method adds a subscriber in the subscriberSet.
     * @param subscriberSet Subscriber Set.
     * @param subscriber subscriber to be added.
     * @param generation subscriber generation.
     * @return updated Subscriber Set.
     */
    public static SubscriberSet add(@NonNull SubscriberSet subscriberSet, @NonNull String subscriber, long generation) {
            ImmutableMap.Builder<String, Long> builder = ImmutableMap.builder();
            builder.putAll(subscriberSet.subscribers);
            builder.put(subscriber, generation);
            return new SubscriberSet(builder.build());
    }

    /**
     * This method updates the generation of a subscriber in the subscriberSet.
     * @param subscriberSet Subscriber Set.
     * @param subscriber subscriber to be added.
     * @param generation subscriber generation.
     * @return updated Subscriber Set.
     */
    public static SubscriberSet update(@NonNull SubscriberSet subscriberSet, @NonNull String subscriber, long generation) {
        ImmutableMap.Builder<String, Long> builder = ImmutableMap.builder();
        Map<String, Long> otherSubscribers = subscriberSet.getSubscribers().entrySet().stream().filter(e -> !e.getKey().equals(subscriber))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        builder.putAll(otherSubscribers);
        builder.put(subscriber, generation);
        return new SubscriberSet(builder.build());
    }

    /**
     * This method removes a subscriber from the subscriberSet.
     * @param subscriberSet Subscriber Set.
     * @param subscriber subscriber to be removed.
     * @return updated Subscriber Set.
     */
    public static SubscriberSet remove(@NonNull SubscriberSet subscriberSet, @NonNull String subscriber) {
       ImmutableMap.Builder<String, Long> builder = ImmutableMap.builder();
       Map<String, Long> otherSubscribers = subscriberSet.getSubscribers().entrySet().stream().filter(e -> e.getKey().equals(subscriber))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
       builder.putAll(otherSubscribers);
       return new SubscriberSet(builder.build());
    }

    private static class SubscriberSetBuilder implements ObjectBuilder<SubscriberSet> {
    }

    @SneakyThrows(IOException.class)
    public static SubscriberSet fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    private static class SubscriberSetSerializer
            extends VersionedSerializer.WithBuilder<SubscriberSet, SubscriberSet.SubscriberSetBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, SubscriberSet.SubscriberSetBuilder recordBuilder)
                throws IOException {
            ImmutableMap.Builder<String, Long> builder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readUTF, DataInput::readLong, builder);
            recordBuilder.subscribers(builder.build());
        }

        private void write00(SubscriberSet subscribersRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeMap(subscribersRecord.getSubscribers(), DataOutput::writeUTF, DataOutput::writeLong);
        }

        @Override
        protected SubscriberSet.SubscriberSetBuilder newBuilder() {
            return SubscriberSet.builder();
        }
    }
}
