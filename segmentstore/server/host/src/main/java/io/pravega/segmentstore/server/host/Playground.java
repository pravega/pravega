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
import io.pravega.common.Timer;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.datastore.DirectMemoryStore;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import io.pravega.segmentstore.storage.mocks.InMemoryCache;
import java.util.ArrayList;
import java.util.Random;
import lombok.Cleanup;
import lombok.Data;
import lombok.RequiredArgsConstructor;
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

        final int entrySize = 10 * 1024;
        final int keyCount = 1000000;
        final int iterationCount = 5;
        final int randomCount = 1_000_000;
        @Cleanup
        val s = new DirectMemoryStore(16 * 1024 * 1024 * 1024L);
        for (int i = 0; i < iterationCount; i++) {
            //val r = testInMemoryCache(entrySize, keyCount);
            //val r = testRocksDbCache(entrySize, keyCount);
            //val r = testDirectStore(s, entrySize, keyCount);
            //System.out.println(String.format("Insert: %d, Get: %d, Remove: %d", r.insertMillis, r.getMillis, r.removeMillis));

            //val r2 = testInMemoryCacheRandom(entrySize, randomCount);
            //val r2 = testRocksDbCacheRandom(entrySize, randomCount);
            val r2 = testDirectStoreRandom(s, entrySize, randomCount);
            System.out.println(String.format("Random operations %d", r2));
        }
    }

    private static Result testInMemoryCache(int bufSize, int count) {
        @Cleanup
        val c = new InMemoryCache("Test");
        return testClassicCache(c, bufSize, count);
    }

    private static Result testRocksDbCache(int bufSize, int count) {
        @Cleanup
        val f = new RocksDBCacheFactory(RocksDBConfig.builder()
                                                     .with(RocksDBConfig.READ_CACHE_SIZE_MB, 512)
                                                     .with(RocksDBConfig.WRITE_BUFFER_SIZE_MB, 256)
                                                     .build());
        @Cleanup
        val c = f.getCache("Test");
        return testClassicCache(c, bufSize, count);
    }

    private static Result testClassicCache(Cache c, int bufSize, int count) {
        val buffer = new ByteArraySegment(new byte[bufSize]);
        new Random(0).nextBytes(buffer.array());
        System.gc();
        val insertTimer = new Timer();
        for (int i = 0; i < count; i++) {
            c.insert(new CacheKey(i), buffer);
        }
        long insertMillis = insertTimer.getElapsedMillis();

        System.gc();
        val getTimer = new Timer();
        for (int i = 0; i < count; i++) {
            c.get(new CacheKey(i));
        }
        long getMillis = getTimer.getElapsedMillis();

        System.gc();
        val removeTimer = new Timer();
        for (int i = 0; i < count; i++) {
            c.remove(new CacheKey(i));
        }
        long removeMillis = removeTimer.getElapsedMillis();

        return new Result(insertMillis, getMillis, removeMillis);
    }

    private static Result testDirectStore(DirectMemoryStore s, int bufSize, int count) throws Exception {
        val buffer = new ByteArraySegment(new byte[bufSize]);
        new Random(0).nextBytes(buffer.array());
        int[] ids = new int[count];

        System.gc();
        val insertTimer = new Timer();
        for (int i = 0; i < count; i++) {
            ids[i] = s.insert(buffer);
        }
        long insertMillis = insertTimer.getElapsedMillis();

        System.gc();
        val getTimer = new Timer();
        for (int i = 0; i < count; i++) {
            BufferView result = s.get(ids[i]);
            byte[] out = result.getCopy();
        }
        long getMillis = getTimer.getElapsedMillis();

        System.gc();
        val removeTimer = new Timer();
        for (int i = 0; i < count; i++) {
            s.delete(ids[i]);
        }
        long removeMillis = removeTimer.getElapsedMillis();

        return new Result(insertMillis, getMillis, removeMillis);
    }

    private static long testInMemoryCacheRandom(int bufSize, int count) {
        @Cleanup
        val c = new InMemoryCache("Test");
        return testClassicCacheRandom(c, bufSize, count);
    }

    private static long testRocksDbCacheRandom(int bufSize, int count) {
        @Cleanup
        val f = new RocksDBCacheFactory(RocksDBConfig.builder()
                                                     .with(RocksDBConfig.READ_CACHE_SIZE_MB, 512)
                                                     .with(RocksDBConfig.WRITE_BUFFER_SIZE_MB, 256)
                                                     .build());
        @Cleanup
        val c = f.getCache("Test");
        return testClassicCacheRandom(c, bufSize, count);
    }

    private static long testClassicCacheRandom(Cache c, int bufSize, int count) {
        val rnd = new Random(0);
        val buffer = new ByteArraySegment(new byte[bufSize]);
        rnd.nextBytes(buffer.array());
        val ids = new ArrayList<Integer>();

        val timer = new Timer();
        for (int i = 0; i < count; i++) {
            boolean insert = (rnd.nextInt(10) < 6) || ids.isEmpty();
            if (insert) {
                int length = rnd.nextInt(buffer.getLength());
                c.insert(new CacheKey(i), buffer.subSegment(0, length));
                ids.add(i);
            } else {
                // delete
                int toRemove = ids.remove(rnd.nextInt(ids.size()));
                c.remove(new CacheKey(toRemove));
            }

            if (!ids.isEmpty()) {
                int toRead = ids.get(rnd.nextInt(ids.size()));
                c.get(new CacheKey(toRead));
            }
        }
        return timer.getElapsedMillis();
    }

    private static long testDirectStoreRandom(int bufSize, int count) throws Exception {
        @Cleanup
        val s = new DirectMemoryStore(16 * 1024 * 1024 * 1024L);
        return testDirectStoreRandom(s, bufSize, count);
    }

    private static long testDirectStoreRandom(DirectMemoryStore s, int bufSize, int count) throws Exception {
        val rnd = new Random(0);
        val buffer = new ByteArraySegment(new byte[bufSize]);
        rnd.nextBytes(buffer.array());
        val ids = new ArrayList<Integer>();

        val timer = new Timer();
        for (int i = 0; i < count; i++) {
            boolean insert = (rnd.nextInt(10) < 6) || ids.isEmpty();
            if (insert) {
                int length = rnd.nextInt(buffer.getLength());
                ids.add(s.insert(buffer.subSegment(0, length)));
            } else {
                // delete
                int toRemove = ids.remove(rnd.nextInt(ids.size()));
                boolean r = s.delete(toRemove);
            }

            if (!ids.isEmpty()) {
                int toRead = ids.get(rnd.nextInt(ids.size()));
                BufferView result = s.get(toRead);
                byte[] out = result.getCopy();
            }
        }

        long elapsed = timer.getElapsedMillis();
        ids.forEach(s::delete); // do not count this.
        return elapsed;
    }

    @RequiredArgsConstructor
    private static class CacheKey extends Cache.Key {
        private final int id;

        @Override
        public byte[] serialize() {
            byte[] r = new byte[4];
            BitConverter.writeInt(r, 0, this.id);
            return r;
        }

        @Override
        public int hashCode() {
            return id;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CacheKey) {
                return this.id == ((CacheKey) obj).id;
            }

            return false;
        }
    }

    @Data
    private static class Result {
        final long insertMillis;
        final long getMillis;
        final long removeMillis;
    }
}
