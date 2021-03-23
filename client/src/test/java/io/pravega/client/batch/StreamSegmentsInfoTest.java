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
package io.pravega.client.batch;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.batch.impl.StreamSegmentsInfoImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import java.util.Arrays;
import java.util.Iterator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StreamSegmentsInfoTest {

    private static final String SCOPE = "scope";
    private static final String STREAM_NAME = "stream";
    @Mock
    private Iterator<SegmentRange> iterator;

    @Test
    public void testStreamSegmentInfoBuilderValid1() {
        StreamSegmentsInfoImpl.builder().startStreamCut(getStreamCut(0, 1, 5, 6))
                              .endStreamCut(getStreamCut(1, 3, 4, 5, 6))
                              .segmentRangeIterator(iterator).build();
    }

    @Test
    public void testStreamSegmentInfoBuilderValid2() {
        StreamSegmentsInfoImpl.builder().startStreamCut(getStreamCut(0L, 3, 4, 5, 6))
                              .endStreamCut(getStreamCut(10L, 3, 4, 7))
                              .segmentRangeIterator(iterator).build();
    }

    @Test(expected = NullPointerException.class)
    public void testStreamSegmentInfoNullCheck() {
        StreamSegmentsInfoImpl.builder().build();
    }

    private StreamCut getStreamCut(long offset, int... segmentNumbers) {
        ImmutableMap.Builder<Segment, Long> segmentMap = ImmutableMap.builder();
        Arrays.stream(segmentNumbers).forEach(
                seg -> segmentMap.put(new Segment(SCOPE, STREAM_NAME, seg), offset));
        return new StreamCutImpl(new StreamImpl(SCOPE, STREAM_NAME), segmentMap.build());
    }
}
