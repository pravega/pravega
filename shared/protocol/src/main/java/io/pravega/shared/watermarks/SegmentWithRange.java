/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared.watermarks;

import java.io.IOException;

import com.google.common.base.Preconditions;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;

/**
 * A serializable representation of a segment with id and range information.  
 */
@Data
public class SegmentWithRange {
    static final SegmentWithRangeSerializer SERIALIZER = new SegmentWithRangeSerializer();
    private final long segmentId;
    private final double rangeLow;
    private final double rangeHigh;

    @Builder
    public SegmentWithRange(long segmentId, double rangeLow, double rangeHigh) {
        Preconditions.checkArgument(segmentId >= 0L);
        Preconditions.checkArgument(rangeLow >= 0.0);
        Preconditions.checkArgument(rangeHigh <= 1.0);
        Preconditions.checkArgument(rangeLow < rangeHigh);
        this.segmentId = segmentId;
        this.rangeLow = rangeLow;
        this.rangeHigh = rangeHigh;
    }

    public static class SegmentWithRangeBuilder implements ObjectBuilder<SegmentWithRange> {

    }
    
    /**
     * Method to check if given segment overlaps with this segment.
     * @param segment segment to check overlap for
     * @return true if they overlap, false otherwise
     */
    public boolean overlaps(final SegmentWithRange segment) {
        return segment.getRangeLow() < rangeHigh && segment.getRangeHigh() > rangeLow;
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
