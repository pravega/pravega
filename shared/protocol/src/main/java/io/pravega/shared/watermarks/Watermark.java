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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

/**
 * Represents a serializable Watermark. 
 * A watermark represents a window on time with corresponding stream cut position that indicates to readers where they in 
 * a stream are vis-a-vis time provided by writers. 
 *
 * The lower time bound is a timestamp which is less than or equal to the most recent value provided by any writer. 
 * The upper time bound is a timestamp which is greater than or equal to any time that were provided by any writer.
 * 
 * Stream cut is an upper bound on most recent positions provided by all writer. 
 */
@Data
public class Watermark {
    public static final WatermarkSerializer SERIALIZER = new WatermarkSerializer();
    public static final Watermark EMPTY = new Watermark(Long.MIN_VALUE, Long.MIN_VALUE, ImmutableMap.of());
    private final long lowerTimeBound;
    private final long upperTimeBound;
    private final Map<SegmentWithRange, Long> streamCut;

    @Builder
    public Watermark(long lowerTimeBound, long upperTimeBound, Map<SegmentWithRange, Long> streamCut) {
        Preconditions.checkArgument(upperTimeBound >= lowerTimeBound);
        this.lowerTimeBound = lowerTimeBound;
        this.upperTimeBound = upperTimeBound;
        this.streamCut = streamCut;
    }

    public static class WatermarkBuilder implements ObjectBuilder<Watermark> {

    }
    
    @SneakyThrows(IOException.class)
    public static Watermark fromByteBuf(final ByteBuffer data) {
        return SERIALIZER.deserialize(new ByteArraySegment(data));
    }

    @SneakyThrows(IOException.class)
    public ByteBuffer toByteBuf() {
        ByteArraySegment serialized = SERIALIZER.serialize(this);
        return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
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
            builder.lowerTimeBound(revisionDataInput.readLong());
            builder.upperTimeBound(revisionDataInput.readLong());
            ImmutableMap.Builder<SegmentWithRange, Long> mapBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(SegmentWithRange.SERIALIZER::deserialize, DataInput::readLong, mapBuilder);
            builder.streamCut(mapBuilder.build());
        }
        
        private void write00(Watermark watermark, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(watermark.lowerTimeBound);
            revisionDataOutput.writeLong(watermark.upperTimeBound);
            revisionDataOutput.writeMap(watermark.streamCut, SegmentWithRange.SERIALIZER::serialize, DataOutput::writeLong);
        }

        @Override
        protected Watermark.WatermarkBuilder newBuilder() {
            return Watermark.builder();
        }
    }
}
