/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.batch;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.StreamCut;
import io.pravega.client.stream.impl.StreamImpl;
import java.util.Arrays;
import java.util.Iterator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StreamSegmentInfoTest {

    private static final String SCOPE = "scope";
    private static final String STREAM_NAME = "stream";
    @Mock
    private Iterator<SegmentInputSplit> iterator;

    @Test(expected = IllegalStateException.class)
    public void testStreamSegmentInvalidStreamCut() {
        StreamSegmentInfo.builder().startStreamCut(getStreamCut(100L, 1)).endStreamCut(getStreamCut(100L, 0))
                         .segmentInputSplitIterator(iterator).build();
    }

    /*
     * Segment map used for the below tests.
     *     +
     *1.0 +--------+--------+-------+------+---------
     *     |        |        |   3          |
     *     |        |   1    |              |
     *     |        |        +-------+------+
     *     |        |        |   4          |
     *     +   0    +--------+-------+------+    8
     *     |        |        |   5   |      |
     *     |        |   2    |       |      |
     *     |        |        +-------+   7  |
     *     |        |        |   6   |      |
     *  0.0+--------+--------+-------+------+---------
     *
     */

    @Test(expected = IllegalStateException.class)
    public void testStreamSegmentInvalidStreamCuts1() {
        //overlapping streamCuts.
        StreamCut startSC = getStreamCut(0L, 1, 5, 6);
        StreamCut endSC = getStreamCut(0, 3, 4, 2);
        StreamSegmentInfo.builder()
                         .startStreamCut(startSC)
                         .endStreamCut(endSC)
                         .segmentInputSplitIterator(iterator).build();
    }

    @Test(expected = IllegalStateException.class)
    public void testStreamSegmentInvalidStreamCuts2() {
        //overlapping streamCuts.
        StreamCut startSC = getStreamCut(0L, 1, 7);
        StreamCut endSC = getStreamCut(0, 3, 4, 2);
        StreamSegmentInfo.builder()
                         .startStreamCut(startSC)
                         .endStreamCut(endSC)
                         .segmentInputSplitIterator(iterator).build();
    }

    @Test
    public void testStreamSegmentInfoBuilderValid1() {
        StreamSegmentInfo.builder().startStreamCut(getStreamCut(0, 1, 5, 6))
                         .endStreamCut(getStreamCut(1, 3, 4, 5, 6))
                         .segmentInputSplitIterator(iterator).build();
    }

    @Test
    public void testStreamSegmentInfoBuilderValid2() {
        StreamSegmentInfo.builder().startStreamCut(getStreamCut(0L, 3, 4, 5, 6))
                         .endStreamCut(getStreamCut(10L, 3, 4, 7))
                         .segmentInputSplitIterator(iterator).build();
    }

    @Test(expected = NullPointerException.class)
    public void testStreamSegmentInfoNullCheck() {
        StreamSegmentInfo.builder().build();
    }

    private StreamCut getStreamCut(long offset, int... segmentNumbers) {
        ImmutableMap.Builder<Segment, Long> segmentMap = ImmutableMap.builder();
        Arrays.stream(segmentNumbers).forEach(
                seg -> segmentMap.put(new Segment(SCOPE, STREAM_NAME, seg), offset));
        return new StreamCut(new StreamImpl(SCOPE, STREAM_NAME), segmentMap.build());
    }
}
