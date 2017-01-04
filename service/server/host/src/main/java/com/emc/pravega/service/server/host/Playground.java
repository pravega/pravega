/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.host;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.emc.pravega.common.concurrent.InlineExecutor;
import com.emc.pravega.common.util.AvlTreeIndex;
import com.emc.pravega.common.util.ByteArraySegment;
import com.emc.pravega.common.util.IndexEntry;
import com.emc.pravega.common.util.RedBlackTreeIndex;
import com.emc.pravega.common.util.SortedIndex;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.containers.StreamSegmentContainerMetadata;
import com.emc.pravega.service.server.reading.CacheManager;
import com.emc.pravega.service.server.reading.CachePolicy;
import com.emc.pravega.service.server.reading.ContainerReadIndex;
import com.emc.pravega.service.server.reading.ReadIndexConfig;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.mocks.InMemoryStorage;
import lombok.Cleanup;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Playground Test class.
 */
public class Playground {

    public static void main(String[] args) throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.INFO);
        context.reset();
        populateIndex();
        //compareTrees();
    }

    private static void populateIndex() throws Exception {
        final int appendSize = 1;
        final int appendCount = 10 * 1000 * 1000;
        final int segmentId = 0;
        final String segmentName = "0";
        final Duration timeout = Duration.ofSeconds(5);
        final byte[] appendData = new byte[appendSize];

        // Create all components.
        System.out.println("Setting up ...");
        ServiceBuilderConfig c = ServiceBuilderConfig.getDefaultConfig();
        UpdateableContainerMetadata metadata = new StreamSegmentContainerMetadata(0);
        @Cleanup("shutdown")
        ScheduledExecutorService executorService = new InlineExecutor();
        @Cleanup
        Storage storage = new InMemoryStorage(executorService);
        @Cleanup
        Cache noOpCache = new Cache() {
            @Override
            public String getId() {
                return "0";
            }

            @Override
            public void insert(Key key, byte[] data) {
            }

            @Override
            public void insert(Key key, ByteArraySegment data) {
            }

            @Override
            public byte[] get(Key key) {
                return new byte[0];
            }

            @Override
            public void remove(Key key) {
            }

            @Override
            public void close() {
            }
        };

        @Cleanup
        CacheManager cacheManager = new CacheManager(new CachePolicy(Long.MAX_VALUE, Duration.ofMillis(Long.MAX_VALUE), Duration.ofMillis(Long.MAX_VALUE)), executorService);
        @Cleanup
        ContainerReadIndex index = new ContainerReadIndex(c.getConfig(ReadIndexConfig::new), metadata, noOpCache, storage, cacheManager, executorService);

        // Setup segment.
        storage.create(segmentName, timeout).join();
        metadata.mapStreamSegmentId(segmentName, segmentId);
        UpdateableSegmentMetadata segmentMetadata = metadata.getStreamSegmentMetadata(segmentId);
        segmentMetadata.setDurableLogLength((long) appendSize * appendCount);

        // Populate with data.
        System.out.println("Appending data ...");
        long currentOffset = 0;
        for (int i = 0; i < appendCount; i++) {
            index.append(segmentId, currentOffset, appendData);
            currentOffset += appendData.length;
        }

        // call GC
        System.out.println("GC ...");
        System.gc();

        // wait for user input.
        System.out.println("Press any key to continue ...");
        System.in.read();
    }

    private static void compareTrees() {
        int count = 10000000;
        int retryCount = 5;
        for (int i = 0; i < retryCount; i++) {
            System.out.print("RB: ");
            System.gc();
            testIndex(new RedBlackTreeIndex<>(Integer::compare), count);
            System.out.println();
            System.out.print("AVL_r:");
            System.gc();
            testIndex(new AvlTreeIndex<>(Integer::compare), count);
            System.out.println();
        }
    }

    private static void testIndex(SortedIndex<Integer, TestEntry> index, int count) {
        long testInsertElapsed = measure(() -> insert(index, 0, count));
        long testReadElapsed = measure(() -> readExact(index, 0, count));
        long testReadCeilingElapsed = measure(() -> readCeiling(index, 0, count));
        System.out.println(String.format("Insert = %sms, Read = %sms, Ceiling = %sms", testInsertElapsed, testReadElapsed, testReadCeilingElapsed));
    }

    private static void insert(SortedIndex<Integer, TestEntry> rbt, int start, int count) {
        int max = start + count;
        for (int i = start; i < max; i++) {
            rbt.put(new TestEntry(i));
        }
    }

    private static void readExact(SortedIndex<Integer, TestEntry> rbt, int start, int count) {
        int max = start + count;
        for (int i = start; i < max; i++) {
            rbt.get(i);
        }
    }

    private static void readCeiling(SortedIndex<Integer, TestEntry> rbt, int start, int count) {
        int max = start + count;
        for (int i = start; i < max; i++) {
            rbt.getCeiling(i);
        }
    }

    private static int measure(Runnable r) {
        long rbtStart = System.nanoTime();
        r.run();
        return (int) ((System.nanoTime() - rbtStart) / 1000 / 1000);
    }

    static class TestEntry implements IndexEntry<Integer> {
        final int key;

        TestEntry(int key) {
            this.key = key;
        }

        @Override
        public Integer key() {
            return this.key;
        }

        @Override
        public String toString() {
            return Integer.toString(this.key);
        }
    }
}
