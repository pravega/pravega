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

import io.pravega.common.MathHelpers;
import io.pravega.common.io.StreamHelpers;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the HandleSerializer class.
 */
public class HandleSerializerTests {
    private static final String SEGMENT_NAME = "Segment";
    private static final SegmentRollingPolicy DEFAULT_ROLLING_POLICY = new SegmentRollingPolicy(1234);

    /**
     * Tests the basic Serialization-Deserialization for a Handle with no concat executed on it.
     */
    @Test
    public void testNormalSerialization() {
        final int subSegmentCount = 1000;
        val source = newHandle(subSegmentCount);
        val serialization = serialize(source);
        val newHandle = HandleSerializer.deserialize(serialization, source.getHeaderHandle());
        assertHandleEquals(source, newHandle, source.getHeaderHandle());
        Assert.assertEquals("getHeaderLength", serialization.length, newHandle.getHeaderLength());
    }

    /**
     * Tests the basic Serialization-Deserialization for a Handle with various successful/unsuccessful concat operations
     * executed on it.
     */
    @Test
    public void testConcat() throws IOException {
        final int subSegmentCount = 10;
        final int concatCount = 15;
        final int failConcatEvery = 2;
        final int addSubSegmentAfterEveryConcat = 3;
        val source = newHandle(subSegmentCount);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        os.write(serialize(source));
        for (int i = 0; i < concatCount; i++) {
            os.write(HandleSerializer.serializeConcat(subSegmentCount, source.length()));
            if (i % failConcatEvery != 0) {

                // Create a handle for the concat segment, and add its info both to the base handle and serialize it.
                val concatHandle = newHandle("concat" + i, subSegmentCount);
                os.write(serialize(concatHandle));
                source.addSubSegments(concatHandle.subSegments().stream()
                        .map(s -> s.withNewOffset(s.getStartOffset() + source.length())).collect(Collectors.toList()));
            }

            // Every now and then, add a new SubSegment to the source, to verify how deserializing Concats (whether successful
            // or not) works with this.
            if (i % addSubSegmentAfterEveryConcat == 0) {
                val subSegment = new SubSegment(StreamSegmentNameUtils.getSubSegmentName(source.getSegmentName(), source.length()), source.length());
                subSegment.setLength(i + 1);
                source.addSubSegments(Collections.singletonList(subSegment));
                os.write(HandleSerializer.serializeSubSegment(subSegment));
            }
        }

        val serialization = os.toByteArray();
        val newHandle = HandleSerializer.deserialize(serialization, source.getHeaderHandle());
        assertHandleEquals(source, newHandle, source.getHeaderHandle());
        Assert.assertEquals("getHeaderLength", serialization.length, newHandle.getHeaderLength());
    }

    private void assertHandleEquals(RollingSegmentHandle expected, RollingSegmentHandle actual, SegmentHandle headerHandle) {
        Assert.assertEquals("getSegmentName", expected.getSegmentName(), actual.getSegmentName());
        AssertExtensions.assertListEquals("subSegments", expected.subSegments(), actual.subSegments(), this::subSegmentsEqual);
        Assert.assertEquals("getRollingPolicy", expected.getRollingPolicy().getMaxLength(), actual.getRollingPolicy().getMaxLength());
        Assert.assertEquals("getHeaderHandle", headerHandle, actual.getHeaderHandle());
        Assert.assertEquals("isReadOnly", headerHandle.isReadOnly(), expected.isReadOnly());
    }

    private boolean subSegmentsEqual(SubSegment s1, SubSegment s2) {
        return s1.getName().equals(s2.getName())
                && s1.getStartOffset() == s2.getStartOffset();
    }

    @SneakyThrows(IOException.class)
    private byte[] serialize(RollingSegmentHandle handle) {
        val s = HandleSerializer.serialize(handle);
        return StreamHelpers.readAll(s.getReader(), s.getLength());
    }

    private RollingSegmentHandle newHandle(int subSegmentCount) {
        return newHandle(SEGMENT_NAME, subSegmentCount);
    }

    private RollingSegmentHandle newHandle(String segmentName, int subSegmentCount) {
        val subSegments = new ArrayList<SubSegment>();
        long offset = 0;
        val rnd = new Random(0);
        for (int i = 0; i < subSegmentCount; i++) {
            val subSegment = new SubSegment(StreamSegmentNameUtils.getSubSegmentName(segmentName, offset), offset);
            subSegment.setLength(MathHelpers.abs(rnd.nextInt()));
            if (i < subSegmentCount - 1) {
                subSegment.markSealed();
            }
            subSegments.add(subSegment);
            offset += subSegment.getLength();
        }

        return new RollingSegmentHandle(new TestHandle(StreamSegmentNameUtils.getHeaderSegmentName(segmentName), false),
                DEFAULT_ROLLING_POLICY, subSegments);
    }

    @Data
    private static class TestHandle implements SegmentHandle {
        private final String segmentName;
        private final boolean readOnly;
    }
}
