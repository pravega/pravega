/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.shared.watermarks.Watermark;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WatermarkReaderImplTest {

    private io.pravega.shared.watermarks.SegmentWithRange convert(SegmentWithRange segment) {
        return new io.pravega.shared.watermarks.SegmentWithRange(segment.getSegment().getSegmentId(), segment.getRange().getLow(), segment.getRange().getHigh());
    }
    
    private Map<io.pravega.shared.watermarks.SegmentWithRange, Long> convert(Map<SegmentWithRange, Long> in) {
        return in.entrySet().stream().collect(Collectors.toMap(e -> convert(e.getKey()), e -> e.getValue()));
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
        assertEquals(0, WatermarkReaderImpl.compare(stream, readerGroupPosition, watermark));
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
    
}
