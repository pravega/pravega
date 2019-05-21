/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.watermarks;

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
import java.nio.ByteBuffer;

@Data
public class Watermark {
    public static final WatermarkSerializer SERIALIZER = new WatermarkSerializer();
    public static final Watermark EMPTY = new Watermark(Long.MIN_VALUE, ImmutableMap.of());
    private final long timestamp;
    private final ImmutableMap<Long, Long> streamCut;

    @Builder
    public Watermark(long timestamp, ImmutableMap<Long, Long> streamCut) {
        this.timestamp = timestamp;
        this.streamCut = streamCut;
    }

    public static class WatermarkBuilder implements ObjectBuilder<Watermark> {

    }
    
    @SneakyThrows(IOException.class)
    public static Watermark fromByteBuf(final ByteBuffer data) {
        return SERIALIZER.deserialize(data.array());
    }

    @SneakyThrows(IOException.class)
    public ByteBuffer toByteBuf() {
        return ByteBuffer.wrap(SERIALIZER.serialize(this).getCopy());
    }

    private static class WatermarkSerializer
            extends VersionedSerializer.WithBuilder<Watermark, Watermark.WatermarkBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput,
                            Watermark.WatermarkBuilder builder) throws IOException {
            builder.timestamp(revisionDataInput.readLong());

            ImmutableMap.Builder<Long, Long> mapBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readLong, DataInput::readLong, mapBuilder);
            builder.streamCut(mapBuilder.build());
        }
        
        private void write00(Watermark watermark, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(watermark.timestamp);
            revisionDataOutput.writeMap(watermark.streamCut, DataOutput::writeLong, DataOutput::writeLong);
        }

        @Override
        protected Watermark.WatermarkBuilder newBuilder() {
            return Watermark.builder();
        }
    }

}
