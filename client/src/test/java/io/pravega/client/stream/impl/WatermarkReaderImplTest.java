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
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.shared.NameUtils;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.test.common.InlineExecutor;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WatermarkReaderImplTest {

    private Map<io.pravega.shared.watermarks.SegmentWithRange, Long> convert(Map<SegmentWithRange, Long> in) {
        return in.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().convert(), e -> e.getValue()));
    }
    
    @Test
    public void testCompare() {
        Stream stream = new StreamImpl("Scope", "streamName");
        SegmentWithRange s0 = new SegmentWithRange(new Segment(stream.getScope(), stream.getStreamName(), 0), 0, 0.5);
        SegmentWithRange s1 = new SegmentWithRange(new Segment(stream.getScope(), stream.getStreamName(), 1), 0.5, 1);
        SegmentWithRange s2 = new SegmentWithRange(new Segment(stream.getScope(), stream.getStreamName(), 2), 0, 0.5);
        SegmentWithRange s3 = new SegmentWithRange(new Segment(stream.getScope(), stream.getStreamName(), 3), 0.5, 1);
        Map<SegmentWithRange, Long> readerGroupPosition = ImmutableMap.of(s1, 1L, s2, 2L);
        Watermark watermark = Watermark.builder().streamCut(convert(readerGroupPosition)).build();
        assertEquals(1, WatermarkReaderImpl.compare(stream, readerGroupPosition, watermark));
        Map<SegmentWithRange, Long> before = ImmutableMap.of(s0, 0L, s1, 1L);
        assertEquals(-1, WatermarkReaderImpl.compare(stream, before, watermark));
        before = ImmutableMap.of(s1, 0L, s2, 1L);
        assertEquals(-1, WatermarkReaderImpl.compare(stream, before, watermark));
        before = ImmutableMap.of(s1, 1L, s2, 1L);
        assertEquals(-1, WatermarkReaderImpl.compare(stream, before, watermark));
        
        Map<SegmentWithRange, Long> after = ImmutableMap.of(s2, 2L, s3, 3L);
        assertEquals(1, WatermarkReaderImpl.compare(stream, after, watermark));
        after = ImmutableMap.of(s1, 2L, s2, 3L);
        assertEquals(1, WatermarkReaderImpl.compare(stream, after, watermark));
        after = ImmutableMap.of(s1, 1L, s2, 3L);
        assertEquals(1, WatermarkReaderImpl.compare(stream, after, watermark));
        
        Map<SegmentWithRange, Long> overlap = ImmutableMap.of(s0, 0L, s3, 3L);
        assertEquals(0, WatermarkReaderImpl.compare(stream, overlap, watermark));
        overlap = ImmutableMap.of(s1, 0L, s2, 3L);
        assertEquals(0, WatermarkReaderImpl.compare(stream, overlap, watermark));
        overlap = ImmutableMap.of(s1, 3L, s2, 0L);
        assertEquals(0, WatermarkReaderImpl.compare(stream, overlap, watermark));   
    }
    
    @Test
    public void testUpdates() {
        Stream stream = new StreamImpl("Scope", "streamName");
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory("Scope", segmentStreamFactory);
        String markStream = NameUtils.getMarkStreamForStream("streamName");
        createScopeAndStream("Scope", markStream, clientFactory.getController());
        RevisionedStreamClient<Watermark> writer = clientFactory.createRevisionedStreamClient(markStream,
                                                                                              new WatermarkSerializer(),
                                                                                              SynchronizerConfig.builder().build());
        InlineExecutor executor = new InlineExecutor();
        @Cleanup
        WatermarkReaderImpl impl = new WatermarkReaderImpl(stream, writer, executor);

        SegmentWithRange s0 = new SegmentWithRange(new Segment(stream.getScope(), stream.getStreamName(), 0), 0, 0.5);
        SegmentWithRange s1 = new SegmentWithRange(new Segment(stream.getScope(), stream.getStreamName(), 1), 0.5, 1);
        SegmentWithRange s2 = new SegmentWithRange(new Segment(stream.getScope(), stream.getStreamName(), 2), 0, 0.5);
        SegmentWithRange s3 = new SegmentWithRange(new Segment(stream.getScope(), stream.getStreamName(), 3), 0.5, 1);
        
        Map<SegmentWithRange, Long> m1 = ImmutableMap.of(s0, 0L, s1, 0L);
        Map<SegmentWithRange, Long> m2 = ImmutableMap.of(s0, 2L, s1, 0L);
        Map<SegmentWithRange, Long> m3 = ImmutableMap.of(s0, 2L, s1, 2L);
        Map<SegmentWithRange, Long> m4 = ImmutableMap.of(s2, 0L, s1, 2L);
        Map<SegmentWithRange, Long> m5 = ImmutableMap.of(s2, 4L, s1, 2L);
        Map<SegmentWithRange, Long> m6 = ImmutableMap.of(s2, 4L, s1, 4L);
        Map<SegmentWithRange, Long> m7 = ImmutableMap.of(s2, 4L, s3, 0L);
        Map<SegmentWithRange, Long> m8 = ImmutableMap.of(s2, 6L, s3, 4L);
        
        writer.writeUnconditionally(Watermark.builder().streamCut(convert(m1)).lowerTimeBound(10).upperTimeBound(19).build());
        writer.writeUnconditionally(Watermark.builder().streamCut(convert(m2)).lowerTimeBound(20).upperTimeBound(29).build());
        writer.writeUnconditionally(Watermark.builder().streamCut(convert(m3)).lowerTimeBound(30).upperTimeBound(39).build());
        writer.writeUnconditionally(Watermark.builder().streamCut(convert(m4)).lowerTimeBound(40).upperTimeBound(49).build());
        writer.writeUnconditionally(Watermark.builder().streamCut(convert(m5)).lowerTimeBound(50).upperTimeBound(59).build());
        writer.writeUnconditionally(Watermark.builder().streamCut(convert(m6)).lowerTimeBound(60).upperTimeBound(69).build());
        writer.writeUnconditionally(Watermark.builder().streamCut(convert(m7)).lowerTimeBound(70).upperTimeBound(79).build());
        writer.writeUnconditionally(Watermark.builder().streamCut(convert(m8)).lowerTimeBound(80).upperTimeBound(89).build());
  
        assertEquals(null, impl.getTimeWindow().getLowerTimeBound());
        assertEquals(null, impl.getTimeWindow().getUpperTimeBound());
        impl.advanceTo(ImmutableMap.of(s0, 1L, s1, 0L));
        assertEquals(10, impl.getTimeWindow().getLowerTimeBound().longValue());
        assertEquals(29, impl.getTimeWindow().getUpperTimeBound().longValue());
        impl.advanceTo(ImmutableMap.of(s0, 3L, s1, 0L));
        assertEquals(20, impl.getTimeWindow().getLowerTimeBound().longValue());
        assertEquals(49, impl.getTimeWindow().getUpperTimeBound().longValue());
        impl.advanceTo(ImmutableMap.of(s0, 5L, s1, 0L));
        assertEquals(20, impl.getTimeWindow().getLowerTimeBound().longValue());
        assertEquals(49, impl.getTimeWindow().getUpperTimeBound().longValue());
        impl.advanceTo(ImmutableMap.of(s0, 6L, s1, 0L));
        assertEquals(20, impl.getTimeWindow().getLowerTimeBound().longValue());
        assertEquals(49, impl.getTimeWindow().getUpperTimeBound().longValue());
        impl.advanceTo(ImmutableMap.of(s0, 6L, s1, 1L));
        assertEquals(20, impl.getTimeWindow().getLowerTimeBound().longValue());
        assertEquals(49, impl.getTimeWindow().getUpperTimeBound().longValue());
        impl.advanceTo(ImmutableMap.of(s0, 6L, s1, 3L));
        assertEquals(30, impl.getTimeWindow().getLowerTimeBound().longValue());
        assertEquals(69, impl.getTimeWindow().getUpperTimeBound().longValue());
        impl.advanceTo(ImmutableMap.of(s2, 0L, s1, 3L));
        assertEquals(40, impl.getTimeWindow().getLowerTimeBound().longValue());
        assertEquals(69, impl.getTimeWindow().getUpperTimeBound().longValue());
        impl.advanceTo(ImmutableMap.of(s2, 4L, s1, 3L));
        assertEquals(50, impl.getTimeWindow().getLowerTimeBound().longValue());
        assertEquals(69, impl.getTimeWindow().getUpperTimeBound().longValue());
        impl.advanceTo(ImmutableMap.of(s2, 4L, s1, 5L));
        assertEquals(60, impl.getTimeWindow().getLowerTimeBound().longValue());
        assertEquals(79, impl.getTimeWindow().getUpperTimeBound().longValue());
        impl.advanceTo(ImmutableMap.of(s2, 4L, s3, 1L));
        assertEquals(70, impl.getTimeWindow().getLowerTimeBound().longValue());
        assertEquals(89, impl.getTimeWindow().getUpperTimeBound().longValue());
        impl.advanceTo(ImmutableMap.of(s2, 5L, s3, 1L));
        assertEquals(70, impl.getTimeWindow().getLowerTimeBound().longValue());
        assertEquals(89, impl.getTimeWindow().getUpperTimeBound().longValue());
        impl.advanceTo(ImmutableMap.of(s2, 5L, s3, 5L));
        assertEquals(70, impl.getTimeWindow().getLowerTimeBound().longValue());
        assertEquals(89, impl.getTimeWindow().getUpperTimeBound().longValue());
        impl.advanceTo(ImmutableMap.of(s2, 6L, s3, 5L));
        assertEquals(80, impl.getTimeWindow().getLowerTimeBound().longValue());
        assertEquals(null, impl.getTimeWindow().getUpperTimeBound());
        impl.advanceTo(ImmutableMap.of(s2, 7L, s3, 7L));
        assertEquals(80, impl.getTimeWindow().getLowerTimeBound().longValue());
        assertEquals(null, impl.getTimeWindow().getUpperTimeBound());
    }
    

    private void createScopeAndStream(String scope, String stream, Controller controller) {
        controller.createScope(scope).join();
        controller.createStream(scope, stream,
                                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
    }
    
}
