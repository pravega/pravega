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
package io.pravega.client;


import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.shared.watermarks.SegmentWithRange;
import io.pravega.shared.watermarks.Watermark;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClientUtilsTest {
    @Test
    public void testGetStreamCutFromWaterMark() {
        SegmentWithRange segmentWithRange1 = new SegmentWithRange(0L, 0.0, 0.5);
        SegmentWithRange segmentWithRange2 = new SegmentWithRange(1L, 0.5, 1.0);
        ImmutableMap<SegmentWithRange, Long> map = ImmutableMap.of(segmentWithRange1, 1L, segmentWithRange2, 1L);
        Watermark watermark = new Watermark(0L, 1L, map, "scope", "stream");
        StreamCutImpl sc = (StreamCutImpl) ClientUtils.getStreamCutFromWaterMark(watermark);
        assertEquals(sc.getStream().getStreamName(), "stream");
        assertEquals(sc.getStream().getScopedName(), "scope/stream");
        assertEquals(sc.getPositions().size(), 2);
    }
}
