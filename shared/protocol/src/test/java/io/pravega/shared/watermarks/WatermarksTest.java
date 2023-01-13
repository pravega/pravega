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

import com.google.common.collect.ImmutableMap;
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class WatermarksTest {
    @Test
    public void testSegmentWithRange() throws IOException {
        SegmentWithRange segmentWithRange = new SegmentWithRange(0L, 0.0, 1.0);
        byte[] serialized = SegmentWithRange.SERIALIZER.serialize(segmentWithRange).getCopy();
        SegmentWithRange deserialized = SegmentWithRange.SERIALIZER.deserialize(serialized);
        assertEquals(segmentWithRange, deserialized);
        AssertExtensions.assertThrows("Exception must be thrown", () -> new SegmentWithRange(-1, 0.0, 1.0), 
                e -> e instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("Exception must be thrown", () -> new SegmentWithRange(0L, -0.1, 1.0), 
                e -> e instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("Exception must be thrown", () -> new SegmentWithRange(0L, 0.0, 1.1), 
                e -> e instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("Exception must be thrown", () -> new SegmentWithRange(0L, 2.0, 1.0), 
                e -> e instanceof IllegalArgumentException);
    }
    
    @Test
    public void testWatermark() {
        SegmentWithRange segmentWithRange1 = new SegmentWithRange(0L, 0.0, 0.5);
        SegmentWithRange segmentWithRange2 = new SegmentWithRange(1L, 0.5, 1.0);
        ImmutableMap<SegmentWithRange, Long> map = ImmutableMap.of(segmentWithRange1, 1L, segmentWithRange2, 1L);
        Watermark watermark = new Watermark(0L, 1L, map);
        ByteBuffer serialized = watermark.toByteBuf();
        Watermark deserialized = Watermark.fromByteBuf(serialized);
        assertEquals(deserialized, watermark);
        AssertExtensions.assertThrows("upper bound less than lower bound", () -> new Watermark(1L, 0L, map),
                e -> e instanceof IllegalArgumentException);
    }
}
