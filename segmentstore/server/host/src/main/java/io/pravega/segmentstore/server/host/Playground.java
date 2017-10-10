/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.segmentstore.storage.rolling.SegmentRollingPolicy;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Executors;
import lombok.Cleanup;
import lombok.val;
import org.slf4j.LoggerFactory;

/**
 * Playground Test class.
 */
public class Playground {

    public static void main(String[] args) throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.INFO);
        //context.reset();

        testRollingStorage();
        testRollingStorageConcat();
    }

    private static void testRollingStorageConcat() throws Exception {
        val targetName = "Target";
        val sourceName = "Source";
        val rollingPolicy = new SegmentRollingPolicy(1000);
        val timeout = Duration.ofSeconds(10);
        val writeSize = 21;

        @Cleanup("shutdown")
        val executor = Executors.newScheduledThreadPool(10);
        val ms = new InMemoryStorage();
        val rsSync = new RollingStorage(ms, rollingPolicy);
        @Cleanup
        val rs = new AsyncStorageWrapper(rsSync, executor);
        rs.initialize(1);

        rs.create(targetName, timeout).join();
        rs.create(sourceName, timeout).join();
        val targetHandle = rs.openWrite(targetName).join();
        val sourceHandle = rs.openWrite(sourceName).join();

        // Write with rollover
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ByteArrayOutputStream cos = new ByteArrayOutputStream();
        long offset = 0;
        val rnd = new Random(0);
        for (int i = 0; i < 10; i++) {
            byte[] data = new byte[writeSize];
            rnd.nextBytes(data);
            rs.write(targetHandle, offset, new ByteArrayInputStream(data), data.length, timeout).join();
            os.write(data);

            rnd.nextBytes(data);
            rs.write(sourceHandle, offset, new ByteArrayInputStream(data), data.length, timeout).join();
            cos.write(data);
            offset += data.length;
        }

        rs.seal(sourceHandle, timeout).join();
        rs.concat(targetHandle, offset, sourceName, timeout).join();
        byte[] sourceData = cos.toByteArray();
        os.write(sourceData);
        cos.close();
        offset += sourceData.length;

        for (int i = 0; i < 10; i++) {
            byte[] data = new byte[writeSize];
            rnd.nextBytes(data);
            rs.write(targetHandle, offset, new ByteArrayInputStream(data), data.length, timeout).join();
            os.write(data);
            offset += data.length;
        }

        val newWriteHandle = rs.openWrite(targetName).join();
        for (int i = 0; i < 10; i++) {
            byte[] data = new byte[writeSize];
            rnd.nextBytes(data);
            rs.write(newWriteHandle, offset, new ByteArrayInputStream(data), data.length, timeout).join();
            os.write(data);
            offset += data.length;
        }

        byte[] writtenData = os.toByteArray();
        byte[] readBuffer = new byte[(int) offset];
        val readhandle = rs.openRead(targetName).join();
        int bytesRead = rs.read(readhandle, 0, readBuffer, 0, readBuffer.length, timeout).join();
        assert bytesRead == readBuffer.length;
        for (int j = 0; j < readBuffer.length; j++) {
            assert writtenData[j] == readBuffer[j] : "read mismatch at offset " + j;
        }

        System.out.println();
    }

    private static void testRollingStorage() throws IOException {
        val segmentName = "Segment";
        val rollingPolicy = new SegmentRollingPolicy(10);
        val timeout = Duration.ofSeconds(10);
        val writeSize = 21;

        @Cleanup("shutdown")
        val executor = Executors.newScheduledThreadPool(10);
        val ms = new InMemoryStorage();
        val rsSync = new RollingStorage(ms, rollingPolicy);
        @Cleanup
        val rs = new AsyncStorageWrapper(rsSync, executor);
        rs.initialize(1);

        // Create
        rs.create(segmentName, timeout).join();
        val si1 = rs.getStreamSegmentInfo(segmentName, timeout).join();
        System.out.println("Create: " + si1);

        // OpenWrite
        val writeHandle = rs.openWrite(segmentName).join();

        // Write with rollover
        long offset = 0;
        val rnd = new Random(0);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for (int i = 0; i < 100; i++) {
            byte[] data = new byte[writeSize];
            rnd.nextBytes(data);
            rs.write(writeHandle, offset, new ByteArrayInputStream(data), data.length, timeout).join();
            offset += data.length;
            os.write(data);
        }

        // Read
        byte[] writtenData = os.toByteArray();
        for (int readOffset = 0; readOffset < offset / 2; readOffset++) {
            int readLength = (int) (offset - 2 * readOffset);
            byte[] readBuffer = new byte[readLength];
            int bytesRead = rs.read(writeHandle, readOffset, readBuffer, 0, readLength, timeout).join();
            assert bytesRead == readLength;
            for (int j = 0; j < readBuffer.length; j++) {
                assert writtenData[readOffset + j] == readBuffer[j] : "ReadOffset = " + readOffset + ", readBuffer.offset = " + j;
            }
        }

        // Truncate
        for (long truncationOffset = 0; truncationOffset < offset; truncationOffset++) {
            rs.truncate(writeHandle, truncationOffset, timeout).join();
            if (truncationOffset % 100 == 0) {
                val si = rs.getStreamSegmentInfo(segmentName, timeout).join();
                System.out.println("Truncate[@" + truncationOffset + "]: " + si);
            }
        }

        // Seal
        rs.seal(writeHandle, timeout).join();
        val si2 = rs.getStreamSegmentInfo(segmentName, timeout).join();
        System.out.println("Seal: " + si2);

        // Delete & Exists
        System.out.println("PreDelete-Exists: " + rs.exists(segmentName, timeout).join());
        rs.delete(writeHandle, timeout).join();
        System.out.println("Delete-Exists: " + rs.exists(segmentName, timeout).join());

        System.out.println();
    }
}
