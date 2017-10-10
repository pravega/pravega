/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.rolling;

import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import lombok.Data;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the RollingSegmentHandle class.
 */
public class RollingSegmentHandleTests {
    private static final SegmentRollingPolicy DEFAULT_ROLLING_POLICY = new SegmentRollingPolicy(1234);
    private static final String SEGMENT_NAME = "Segment";
    private static final String HEADER_NAME = StreamSegmentNameUtils.getHeaderSegmentName(SEGMENT_NAME);

    /**
     * Tests various features of the RollingSegmentHandle.
     */
    @Test
    public void testMainFeatures() {
        val headerHandle = new TestHandle(HEADER_NAME, true);
        val h = new RollingSegmentHandle(headerHandle, DEFAULT_ROLLING_POLICY, new ArrayList<>());
        Assert.assertNull("Unexpected lastSubSegment for empty handle.", h.lastSubSegment());
        Assert.assertEquals("Unexpected contents in subSegments() for empty handle.", 0, h.subSegments().size());
        Assert.assertEquals("Unexpected value for length() for empty handle.", 0, h.length());
        Assert.assertEquals("Unexpected value for getHeaderLength() for empty handle.", 0, h.getHeaderLength());
        Assert.assertEquals("Unexpected value for getHeaderHandle().", headerHandle, h.getHeaderHandle());
        Assert.assertEquals("Unexpected segment name.", SEGMENT_NAME, h.getSegmentName());
        Assert.assertEquals("Unexpected rolling policy.", DEFAULT_ROLLING_POLICY, h.getRollingPolicy());
        Assert.assertTrue("Unexpected value for isReadOnly.", h.isReadOnly());
        Assert.assertFalse("Unexpected value for isSealed.", h.isSealed());

        // Active handles.
        AssertExtensions.assertThrows(
                "setActiveSubSegmentHandle accepted a handle when no subsegments are registered.",
                () -> h.setActiveSubSegmentHandle(new TestHandle("foo", false)),
                ex -> ex instanceof IllegalStateException);

        val subSegmentName = "subsegment";
        h.addSubSegment(new SubSegment(subSegmentName, 0L), new TestHandle(subSegmentName, false));
        AssertExtensions.assertThrows(
                "setActiveSubSegmentHandle accepted a handle that does not match the last subsegment name.",
                () -> h.setActiveSubSegmentHandle(new TestHandle("foo", false)),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "setActiveSubSegmentHandle accepted a read-only handle.",
                () -> h.setActiveSubSegmentHandle(new TestHandle(subSegmentName, true)),
                ex -> ex instanceof IllegalArgumentException);

        val activeHandle = new TestHandle(subSegmentName, false);
        h.setActiveSubSegmentHandle(activeHandle);
        Assert.assertEquals("Unexpected value from getActiveSubSegmentHandle.", activeHandle, h.getActiveSubSegmentHandle());

        // Header length.
        h.setHeaderLength(10);
        h.increaseHeaderLength(5);
        Assert.assertEquals("Unexpected value for getHeaderLength when set and then increased.", 15, h.getHeaderLength());

        // Sealed.
        h.markSealed();
        Assert.assertTrue("Unexpected value for isSealed.", h.isSealed());
    }

    /**
     * Tests the ability to add a single SubSegment.
     */
    @Test
    public void testAddSubSegment() {
        val headerHandle = new TestHandle(HEADER_NAME, true);
        val h = new RollingSegmentHandle(headerHandle, DEFAULT_ROLLING_POLICY, new ArrayList<>());

        AssertExtensions.assertThrows(
                "addSubSegment allowed adding a null ActiveSegmentHandle.",
                () -> h.addSubSegment(new SubSegment("s", 0L), null),
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows(
                "addSubSegment allowed adding a read-only ActiveSegmentHandle.",
                () -> h.addSubSegment(new SubSegment("s", 0L), new TestHandle("s", true)),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "addSubSegment allowed adding an ActiveSegmentHandle with different name..",
                () -> h.addSubSegment(new SubSegment("s", 0L), new TestHandle("s2", false)),
                ex -> ex instanceof IllegalArgumentException);
        Assert.assertEquals("Not expecting any SubSegments to be added.", 0, h.subSegments().size());
        Assert.assertNull("Not expecting the Active SubSegment handle to be set.", h.getActiveSubSegmentHandle());

        val subSegment = new SubSegment("s1", 100L);
        h.addSubSegment(subSegment, new TestHandle("s1", false));
        subSegment.setLength(123L);
        Assert.assertEquals("Unexpected value for length() after adding one SubSegment.",
                subSegment.getStartOffset() + subSegment.getLength(), h.length());
        AssertExtensions.assertListEquals("Unexpected contents for subSegments().",
                Collections.singletonList(subSegment), h.subSegments(), Object::equals);
        Assert.assertEquals("Unexpected lastSubSegment.", subSegment, h.lastSubSegment());

        AssertExtensions.assertThrows("addSubSegment allowed adding a SubSegment that is not contiguous.",
                () -> h.addSubSegment(new SubSegment("s2", subSegment.getLastOffset() + 1), new TestHandle("s2", false)),
                ex -> ex instanceof IllegalArgumentException);

        subSegment.markInexistent();
        val subSegment2 = new SubSegment("s2", subSegment.getLastOffset());
        subSegment2.setLength(234L);
        h.addSubSegment(subSegment2, new TestHandle("s2", false));
        Assert.assertEquals("Unexpected number of registered SubSegments.", 2, h.subSegments().size());
        Assert.assertEquals("Unexpected value for length() after adding two SubSegment.",
                subSegment2.getStartOffset() + subSegment2.getLength(), h.length());
        Assert.assertEquals("Unexpected lastSubSegment.", subSegment2, h.lastSubSegment());
    }


    /**
     * Tests the ability to add multiple SubSegments at once, atomically.
     */
    @Test
    public void testAddSubSegments() {
        val headerHandle = new TestHandle(HEADER_NAME, true);
        val h = new RollingSegmentHandle(headerHandle, DEFAULT_ROLLING_POLICY, new ArrayList<>());

        val firstBadList = Arrays.asList(
                new SubSegment("s1", 0L),
                new SubSegment("s2", 10L));
        firstBadList.get(0).setLength(9);
        AssertExtensions.assertThrows(
                "addSubSegments allowed an incontiguous list of SubSegments to be added.",
                () -> h.addSubSegments(firstBadList),
                ex -> ex instanceof IllegalArgumentException);
        Assert.assertEquals("Not expecting any SubSegments to be added.", 0, h.subSegments().size());
        Assert.assertEquals("Unexpected length().", 0, h.length());

        val validList = Arrays.asList(
                new SubSegment("s1", 0L),
                new SubSegment("s2", 10L));
        validList.get(0).setLength(10);
        validList.get(1).setLength(5);
        h.addSubSegments(validList);
        AssertExtensions.assertListEquals("Unexpected list of SubSegments.", validList, h.subSegments(), Object::equals);
        Assert.assertEquals("Unexpected length.", 15, h.length());

        val secondBadList = Arrays.asList(
                new SubSegment("s3", h.length() - 1),
                new SubSegment("s4", h.length() + 1));
        secondBadList.get(0).setLength(2);
        AssertExtensions.assertThrows(
                "addSubSegments allowed an incontiguous list of SubSegments to be added.",
                () -> h.addSubSegments(secondBadList),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Tests the ability of the Handle to refresh based on information from another similar handle.
     */
    @Test
    public void testRefresh() {
        val headerHandle = new TestHandle(HEADER_NAME, true);
        val target = new RollingSegmentHandle(headerHandle, DEFAULT_ROLLING_POLICY,
                Collections.singletonList(new SubSegment("s1", 0L)));

        val source = new RollingSegmentHandle(headerHandle, DEFAULT_ROLLING_POLICY, Arrays.asList(
                new SubSegment("s1", 0L),
                new SubSegment("s2", 100L)));
        source.subSegments().get(0).setLength(100);
        source.markSealed();
        source.setHeaderLength(1000);
        source.setActiveSubSegmentHandle(new TestHandle(source.lastSubSegment().getName(), false));

        target.refresh(source);
        Assert.assertEquals("Unexpected getHeaderLength()", source.getHeaderLength(), target.getHeaderLength());
        AssertExtensions.assertListEquals("Unexpected subSegments()", source.subSegments(), target.subSegments(), Object::equals);
        Assert.assertTrue("Unexpected isSealed.", target.isSealed());
        Assert.assertNull("Not expecting any ActiveSegmentHandle to be copied.", target.getActiveSubSegmentHandle());
    }

    @Data
    private static class TestHandle implements SegmentHandle {
        private final String segmentName;
        private final boolean readOnly;
    }
}
