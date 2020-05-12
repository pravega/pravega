/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import com.google.common.collect.Iterators;
import io.pravega.common.io.StreamHelpers;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

public class CompositeBufferViewTests {
    private static final int COMPONENT_COUNT = 10;

    /**
     * Tests {@link BufferView#wrap(List)}.
     */
    @Test
    public void testWrap() throws IOException {
        val empty = BufferView.wrap(Collections.emptyList());
        Assert.assertTrue(empty instanceof ByteArraySegment);
        Assert.assertEquals(0, empty.getLength());

        val b1 = new ByteArraySegment(new byte[]{1});
        val b2 = new ByteArraySegment(new byte[]{2});
        val single = BufferView.wrap(Collections.singletonList(b1));
        Assert.assertSame(b1, single);

        val composite = BufferView.wrap(Arrays.asList(b1, b2));
        Assert.assertEquals(b1.getLength() + b2.getLength(), composite.getLength());
        AssertExtensions.assertStreamEquals("",
                new SequenceInputStream(b1.getReader(), b2.getReader()), composite.getReader(), composite.getLength());

        val contentBufs = composite.getContents();
        val expectedContentBufs = Stream.of(b1, b2).flatMap(b -> b.getContents().stream()).collect(Collectors.toList());
        AssertExtensions.assertListEquals("", expectedContentBufs, contentBufs, ByteBuffer::equals);
    }

    /**
     * Tests {@link BufferView#wrap(List)}.
     */
    @Test
    public void testWrapRecursive() throws IOException {
        val b1 = new ByteArraySegment(new byte[]{1});
        val b2 = new ByteArraySegment(new byte[]{2});
        val b3 = new ByteArraySegment(new byte[]{3});

        val c1 = BufferView.wrap(Arrays.asList(b1, b2));
        val c2 = BufferView.wrap(Arrays.asList(c1, b3));
        Assert.assertEquals(b1.getLength() + b2.getLength() + b3.getLength(), c2.getLength());
        AssertExtensions.assertStreamEquals("",
                new SequenceInputStream(Iterators.asEnumeration(Arrays.asList(b1.getReader(), b2.getReader(), b3.getReader()).iterator())),
                c2.getReader(), c2.getLength());

        val contentBufs = c2.getContents();
        val expectedContentBufs = Stream.of(b1, b2, b3).flatMap(b -> b.getContents().stream()).collect(Collectors.toList());
        AssertExtensions.assertListEquals("", expectedContentBufs, contentBufs, ByteBuffer::equals);
    }

    /**
     * Tests {@link CompositeBufferView#getReader()}.
     */
    @Test
    public void testGetBufferViewReader() throws IOException {
        val components = createComponents();
        val cb = BufferView.wrap(components);
        val expectedSize = components.stream().mapToInt(BufferView::getLength).sum();
        val asInputStream = new SequenceInputStream(Iterators.asEnumeration(components.stream().map(BufferView::getReader).iterator()));
        val expected = StreamHelpers.readAll(asInputStream, expectedSize);
        val reader = cb.getBufferViewReader();
        val actual = reader.readFully(3);
        AssertExtensions.assertArrayEquals("", expected, 0, actual.array(), actual.arrayOffset(), expectedSize);
        Assert.assertEquals(0, reader.readBytes(new ByteArraySegment(new byte[1])));
    }

    /**
     * Tests {@link CompositeBufferView#getReader()}.
     */
    @Test
    public void testGetReader() throws IOException {
        val components = createComponents();
        val cb = BufferView.wrap(components);
        val expectedSize = components.stream().mapToInt(BufferView::getLength).sum();
        val expected = new SequenceInputStream(Iterators.asEnumeration(components.stream().map(BufferView::getReader).iterator()));
        val actual = cb.getReader();
        AssertExtensions.assertStreamEquals("", expected, actual, expectedSize);
    }

    /**
     * Tests {@link CompositeBufferView#getCopy()}.
     */
    @Test
    public void testGetCopy() throws IOException {
        val components = createComponents();
        val cb = BufferView.wrap(components);
        val expectedSize = components.stream().mapToInt(BufferView::getLength).sum();
        val expected = StreamHelpers.readAll(
                new SequenceInputStream(Iterators.asEnumeration(components.stream().map(BufferView::getReader).iterator())),
                expectedSize);
        val actual = cb.getCopy();
        Assert.assertArrayEquals("", expected, actual);
    }


    /**
     * Tests {@link CompositeBufferView#copyTo(OutputStream)}.
     */
    @Test
    public void testCopyToOutputStream() throws IOException {
        val components = createComponents();
        val cb = BufferView.wrap(components);
        val expectedSize = components.stream().mapToInt(BufferView::getLength).sum();
        val expected = StreamHelpers.readAll(
                new SequenceInputStream(Iterators.asEnumeration(components.stream().map(BufferView::getReader).iterator())),
                expectedSize);
        val actual = new ByteArrayOutputStream();
        cb.copyTo(actual);
        Assert.assertEquals(expectedSize, actual.size());
        Assert.assertArrayEquals("", expected, actual.toByteArray());
    }

    /**
     * Tests {@link CompositeBufferView#copyTo(ByteBuffer)}.
     */
    @Test
    public void testCopyToByteBuffer() throws IOException {
        val components = createComponents();
        val cb = BufferView.wrap(components);
        val expectedSize = components.stream().mapToInt(BufferView::getLength).sum();
        val expected = StreamHelpers.readAll(
                new SequenceInputStream(Iterators.asEnumeration(components.stream().map(BufferView::getReader).iterator())),
                expectedSize);
        val actual = new byte[expectedSize];
        cb.copyTo(ByteBuffer.wrap(actual));
        Assert.assertArrayEquals("", expected, actual);
    }

    /**
     * Tests {@link CompositeBufferView#slice(int, int)} and {@link CompositeBufferView#getReader(int, int)}.
     */
    @Test
    public void testSlice() throws IOException {
        val components = createComponents();
        val cb = BufferView.wrap(components);
        val expectedSize = components.stream().mapToInt(BufferView::getLength).sum();
        val expected = StreamHelpers.readAll(
                new SequenceInputStream(Iterators.asEnumeration(components.stream().map(BufferView::getReader).iterator())),
                expectedSize);
        for (int i = 0; i < expectedSize / 2; i++) {
            int sliceLength = expectedSize - i;
            val slice = cb.slice(i, sliceLength);
            val sliceData = slice.getCopy();
            AssertExtensions.assertArrayEquals("slice(offset, length)", expected, i, sliceData, 0, sliceLength);

            val sliceReader = cb.getReader(i, sliceLength);
            val sliceReaderData = StreamHelpers.readAll(sliceReader, sliceLength);
            AssertExtensions.assertArrayEquals("getReader(offset, length)", expected, i, sliceReaderData, 0, sliceLength);
        }
    }

    /**
     * Tests {@link CompositeBufferView#retain()} and {@link CompositeBufferView#release()}.
     */
    @Test
    public void testRetainRelease() {
        val components = createComponents();
        val cb = BufferView.wrap(components);

        for (val c : components) {
            Assert.assertEquals("Initial.", 0, ((RetainedByteArraySegment) c).refCount);
        }

        cb.retain();
        for (val c : components) {
            Assert.assertEquals("After retain().", 1, ((RetainedByteArraySegment) c).refCount);
        }

        cb.release();
        for (val c : components) {
            Assert.assertEquals("After release().", 0, ((RetainedByteArraySegment) c).refCount);
        }
    }

    private List<BufferView> createComponents() {
        val rnd = new Random(0);
        val result = new ArrayList<BufferView>();
        int size = COMPONENT_COUNT / 2;
        for (int i = 0; i < COMPONENT_COUNT; i++) {
            byte[] b = new byte[Math.abs(size)];
            rnd.nextBytes(b);
            result.add(new RetainedByteArraySegment(b));
        }
        return result;
    }

    private static class RetainedByteArraySegment extends ByteArraySegment {
        private int refCount;

        RetainedByteArraySegment(byte[] array) {
            super(array);
            this.refCount = 0;
        }

        @Override
        public void retain() {
            this.refCount++;
        }

        @Override
        public void release() {
            this.refCount--;
        }
    }
}
