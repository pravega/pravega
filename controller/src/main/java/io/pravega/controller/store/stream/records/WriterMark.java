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

/**
 * This is a data class that represents a writer's mark. 
 * A writer sends a time and its position corresponding to the time.
 * This is data object where the information sent by the writer is recorded and serialized to be stored in underlying 
 * metadata store. 
 */
@Data
public class WriterMark {
    public static final WriterMarkSerializer SERIALIZER = new WriterMarkSerializer();
    public static final WriterMark EMPTY = new WriterMark(Long.MIN_VALUE, ImmutableMap.of());
    private final long timestamp;
    private final ImmutableMap<Long, Long> position;

    @Builder
    public WriterMark(long timestamp, ImmutableMap<Long, Long> position) {
        this.timestamp = timestamp;
        this.position = position;
    }

    public static class WriterMarkBuilder implements ObjectBuilder<WriterMark> {

    }
    
    @SneakyThrows(IOException.class)
    public static WriterMark fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    private static class WriterMarkSerializer
            extends VersionedSerializer.WithBuilder<WriterMark, WriterMark.WriterMarkBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput,
                            WriterMark.WriterMarkBuilder builder) throws IOException {
            builder.timestamp(revisionDataInput.readLong());

            ImmutableMap.Builder<Long, Long> mapBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readLong, DataInput::readLong, mapBuilder);
            builder.position(mapBuilder.build());
        }
        
        private void write00(WriterMark writerMark, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(writerMark.timestamp);
            revisionDataOutput.writeMap(writerMark.position, DataOutput::writeLong, DataOutput::writeLong);
        }

        @Override
        protected WriterMark.WriterMarkBuilder newBuilder() {
            return WriterMark.builder();
        }
    }

}
