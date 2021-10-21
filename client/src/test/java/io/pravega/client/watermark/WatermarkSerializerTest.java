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
package io.pravega.client.watermark;

import com.google.common.collect.ImmutableMap;
import io.pravega.shared.watermarks.SegmentWithRange;
import io.pravega.shared.watermarks.Watermark;
import java.nio.ByteBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WatermarkSerializerTest {
    @Test
    public void testWatermark() {
        SegmentWithRange segmentWithRange1 = new SegmentWithRange(0L, 0.0, 0.5);
        SegmentWithRange segmentWithRange2 = new SegmentWithRange(1L, 0.5, 1.0);
        ImmutableMap<SegmentWithRange, Long> map = ImmutableMap.of(segmentWithRange1, 1L, segmentWithRange2, 1L);
        Watermark watermark = new Watermark(0L, 1L, map);
        WatermarkSerializer serializer = new WatermarkSerializer();
        ByteBuffer serialized = serializer.serialize(watermark);
        Watermark deserialized = serializer.deserialize(serialized);
        assertEquals(watermark, deserialized);
    }
}
