/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.netty.buffer.Unpooled;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.val;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

/**
 * Playground Test class.
 */
public class Playground {

    public static void main(String[] args) throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.INFO);
        //context.reset();
        testBuffers();

        //testArrays();
    }

    private static void testArrays() {
        int count = 1000 * 1000 * 1000;
        byte[] data = new byte[8];
        measure(() -> {
            for (int i = 0; i < count; i++) {
                val bb = ByteBuffer.wrap(data);
                bb.putLong(0, 1234567890L);
                bb.putInt(0, 123456789);
            }
            return count;
        }, "");

        measure(() -> {
            for (int i = 0; i < count; i++) {
                BitConverter.writeLong(data, 0, 1234567890L);
                BitConverter.writeInt(data, 0, 123456789);
            }
            return count;
        }, "");
    }


    private static void testBuffers() {
        int bufferCount = 255;
        int bufferSize = 100 * 1024;
        int totalTestCount = 100;
        val rawBuffers = new ArrayList<byte[]>();
        val random = new Random(0);
        for (int i = 0; i < bufferCount; i++) {
            val b = new byte[bufferSize];
            random.nextBytes(b);
            rawBuffers.add(b);
        }

        val byteArraySegments = toByteArraySegments(rawBuffers);
        val byteBufs = toByteBufs(rawBuffers);
        val compositeBufs = toCompositeBufs(rawBuffers, random);

        measure(() -> testEquality(byteArraySegments, totalTestCount), "Equals(ByteArraySegment)");
        measure(() -> testEquality(byteBufs, totalTestCount), "Equals(ByteBufWrapper)");
        measure(() -> testEquality(compositeBufs, totalTestCount), "Equals(CompositeBufferView)");

        val comparator = BufferViewComparator.create();
        System.out.println("Comparing using " + comparator.getClass().getSimpleName());
        measure(() -> testCompare(byteArraySegments, comparator, totalTestCount), "Compare(ByteArraySegment)");
        measure(() -> testCompare(byteBufs, comparator, totalTestCount), "Compare(ByteBufWrapper)");
        measure(() -> testCompare(compositeBufs, comparator, totalTestCount), "Compare(CompositeBufferView)");
    }

    private static void measure(Supplier<Integer> toMeasure, String description) {
        System.gc();
        long startTime = System.nanoTime();
        int count = toMeasure.get();
        long elapsed = System.nanoTime() - startTime;
        System.out.println(String.format("%s. Elapsed = %.1f ms, per item = %.1f us / %.1f ns.", description, elapsed / 1000_0000.0, (double) elapsed / count / 1000, (double) elapsed / count));
    }

    private static int testEquality(List<BufferView> bufferViews, int count) {
        for (int testId = 0; testId < count; testId++) {
            for (val b1 : bufferViews) {
                for (val b2 : bufferViews) {
                    b1.equals(b2);
                }
            }
        }
        return count * bufferViews.size() * bufferViews.size();
    }

    private static int testCompare(List<BufferView> bufferViews, BufferViewComparator comparator, int count) {
        for (int testId = 0; testId < count; testId++) {
            for (val b1 : bufferViews) {
                for (val b2 : bufferViews) {
                    comparator.compare(b1, b2);
                }
            }
        }
        return count * bufferViews.size() * bufferViews.size();
    }

    private static List<BufferView> toByteArraySegments(List<byte[]> rawBuffers) {
        return rawBuffers.stream().map(ByteArraySegment::new).collect(Collectors.toList());
    }

    private static List<BufferView> toByteBufs(List<byte[]> rawBuffers) {
        return rawBuffers.stream().map(Unpooled::wrappedBuffer).map(ByteBufWrapper::new).collect(Collectors.toList());
    }

    private static List<BufferView> toCompositeBufs(List<byte[]> rawBuffers, Random random) {
        return rawBuffers.stream().map(b -> {
            val builder = BufferView.builder();
            int offset = 0;
            while (offset < b.length) {
                int nextSliceLength = random.nextInt(b.length - offset) + 1;
                builder.add(new ByteArraySegment(b, offset, nextSliceLength)); // Mix&Match with ByteBufWrapper?
                offset += nextSliceLength;
            }

            return builder.build();
        }).collect(Collectors.toList());
    }
}
