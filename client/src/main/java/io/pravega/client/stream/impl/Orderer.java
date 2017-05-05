/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.SegmentInputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Used to select which event should go next when consuming from multiple segments.
 *
 */
public class Orderer {
    private final AtomicInteger counter = new AtomicInteger(0);

    /**
     * Given a list of segments this reader owns, (which contain their positions) returns the one that should
     * be read from next. This is done in way to minimize blocking and ensure fairness.
     *
     * @param segments The logs to get the next reader for.
     * @return A segment that this reader should read from next.
     */
    SegmentInputStream nextSegment(List<SegmentInputStream> segments) {
        if (segments.isEmpty()) {
            return null;
        }
        for (int i = 0; i < segments.size(); i++) {
            SegmentInputStream inputStream = segments.get(counter.incrementAndGet() % segments.size());
            if (inputStream.canReadWithoutBlocking()) {
                return inputStream;
            } else {
                inputStream.fillBuffer();
            }
        }
        return segments.get(counter.incrementAndGet() % segments.size());
    }
}
