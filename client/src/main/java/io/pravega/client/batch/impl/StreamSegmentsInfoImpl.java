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
package io.pravega.client.batch.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.StreamSegmentsIterator;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.StreamCut;
import java.util.Iterator;
import java.util.Map;
import lombok.Builder;
import lombok.NonNull;
import lombok.ToString;

/**
 * This class contains the segment information of a stream between two StreamCuts.
 */
@ToString
@Builder
public class StreamSegmentsInfoImpl implements StreamSegmentsIterator {

    @NonNull
    private final StreamCut startStreamCut;

    @NonNull
    private final StreamCut endStreamCut;

    @NonNull
    private final Iterator<SegmentRange> segmentRangeIterator;

    @Override
    public Iterator<SegmentRange> getIterator() {
        return segmentRangeIterator;
    }

    @Override
    public StreamCut getStartStreamCut() {
        return startStreamCut;
    }

    @Override
    public StreamCut getEndStreamCut() {
        return endStreamCut;
    }

    @Override
    public StreamSegmentsInfoImpl asImpl() {
        return this;
    }

    public static final class StreamSegmentsInfoImplBuilder {
        public StreamSegmentsInfoImpl build() {
            validateStreamCuts(startStreamCut, endStreamCut);
            return new StreamSegmentsInfoImpl(startStreamCut, endStreamCut, segmentRangeIterator);
        }
    }

    static void validateStreamCuts(final StreamCut startStreamCut, final StreamCut endStreamCut) {
        //Validate that startStreamCut and endStreamCut are for the same stream.
        Preconditions.checkArgument(startStreamCut.asImpl().getStream().equals(endStreamCut.asImpl().getStream()),
                "startStreamCut and endStreamCut should be for the same stream.");

        final Map<Segment, Long> startSegments = startStreamCut.asImpl().getPositions();
        final Map<Segment, Long> endSegments = endStreamCut.asImpl().getPositions();
        //Ensure that the offsets of overlapping segments does not decrease from startStreamCut to endStreamCut.
        startSegments.keySet().stream().filter(endSegments::containsKey)
                     .forEach(s -> Preconditions.checkState(startSegments.get(s) <= endSegments.get(s),
                             "Segment offset in startStreamCut should be <= segment offset in endStreamCut."));
    }
}
