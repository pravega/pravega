/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.watermarks;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A serializable representation of a segment with id and range information.  
 */
@Data
@Builder
public class SegmentWithRange {
    public static final SegmentWithRangeSerializer SERIALIZER = new SegmentWithRangeSerializer();
    private final long segmentId;
    private final double rangeLow;
    private final double rangeHigh;
    
    public static class SegmentWithRangeBuilder implements ObjectBuilder<SegmentWithRange> {

    }
    
    @SneakyThrows(IOException.class)
    public static SegmentWithRange fromByteBuf(final ByteBuffer data) {
        return SERIALIZER.deserialize(data.array());
    }

    @SneakyThrows(IOException.class)
    public ByteBuffer toByteBuf() {
        return ByteBuffer.wrap(SERIALIZER.serialize(this).getCopy());
    }

    static class SegmentWithRangeSerializer
            extends VersionedSerializer.WithBuilder<SegmentWithRange, SegmentWithRangeBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput,
                            SegmentWithRange.SegmentWithRangeBuilder builder) throws IOException {
            builder.segmentId(revisionDataInput.readLong());
            builder.rangeLow(revisionDataInput.readDouble());
            builder.rangeHigh(revisionDataInput.readDouble());
        }
        
        private void write00(SegmentWithRange segmentWithRange, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(segmentWithRange.segmentId);
            revisionDataOutput.writeDouble(segmentWithRange.rangeLow);
            revisionDataOutput.writeDouble(segmentWithRange.rangeHigh);
        }

        @Override
        protected SegmentWithRange.SegmentWithRangeBuilder newBuilder() {
            return SegmentWithRange.builder();
        }
    }
}
