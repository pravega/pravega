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

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.shared.watermarks.SegmentWithRange;
import io.pravega.shared.watermarks.Watermark;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
final class ClientUtils {

    /**
     * Utility method to generate streamCut from given watermark.
     *
     * @param watermark             watermark object.
     * @return A StreamCut object from given Watermark
     */
    public static StreamCut getStreamCutFromWaterMark(Watermark watermark) {
        if (watermark != null && !watermark.getScope().isEmpty() && !watermark.getStream().isEmpty()) {
            Stream stream = Stream.of(watermark.getScope(), watermark.getStream());
            Map<Segment, Long> position = new LinkedHashMap<>();
            for (Map.Entry<SegmentWithRange, Long> entry : watermark.getStreamCut().entrySet()) {
                position.put(new Segment(watermark.getScope(), watermark.getStream(), entry.getKey().getSegmentId()), entry.getValue());
            }
            return new StreamCutImpl(stream, position);
        }
        return null;
    }
}
