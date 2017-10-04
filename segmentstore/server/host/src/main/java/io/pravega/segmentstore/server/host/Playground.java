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
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.segmentstore.storage.rolling.SegmentRollingPolicy;
import java.io.ByteArrayInputStream;
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
    }

    private static void testRollingStorage() {
        val segmentName = "Segment";
        val rollingPolicy = new SegmentRollingPolicy(10);
        val timeout = Duration.ofSeconds(10);
        val writeSize = 21;

        @Cleanup("shutdown")
        val executor = Executors.newScheduledThreadPool(10);
        @Cleanup
        val msf = new InMemoryStorageFactory(executor);
        @Cleanup
        val rs = new RollingStorage(msf, rollingPolicy, executor);
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
        for (int i = 0; i < 100; i++) {
            byte[] data = new byte[writeSize];
            rnd.nextBytes(data);
            rs.write(writeHandle, offset, new ByteArrayInputStream(data), data.length, timeout).join();
            offset += data.length;
        }

        // Concat TODO

        // Read TODO

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
