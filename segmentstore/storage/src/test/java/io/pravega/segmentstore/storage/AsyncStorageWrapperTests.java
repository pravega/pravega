/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for AsyncStorageWrapper class.
 */
public class AsyncStorageWrapperTests extends ThreadPooledTestSuite {

    private static final int TIMEOUT_MILLIS = 1000;
    private static final Duration TIMEOUT = Duration.ofMillis(TIMEOUT_MILLIS);

    // TODO: add global timeout

    @Override
    protected int getThreadPoolSize() {
        return 2;
    }

    /**
     * Tests pass-through functionality (the ability to invoke the appropriate method in the inner SyncStorage).
     */
    @Test
    public void testPassThrough() {
        final String segmentName = "Segment";
        final String concatSourceName = "Concat";
        val handle = InMemoryStorage.newHandle(segmentName, false);
        AtomicReference<Object> toReturn = new AtomicReference<>();
        AtomicReference<BiConsumer<String, String>> validator = new AtomicReference<>();
        val innerStorage = new TestStorage((operation, segment) -> {
            validator.get().accept(operation, segment);
            return toReturn.get();
        });

        val s = new AsyncStorageWrapper(innerStorage, executorService());

        // Create
        toReturn.set(StreamSegmentInformation.builder().name(segmentName).build());
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.CREATE, o);
            Assert.assertEquals(segmentName, segment);
        });
        val createResult = s.create(segmentName, TIMEOUT).join();
        Assert.assertEquals(toReturn.get(), createResult);

        // Delete
        toReturn.set(null);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.DELETE, o);
            Assert.assertEquals(segmentName, segment);
        });
        s.delete(handle, TIMEOUT).join();

        // OpenRead
        toReturn.set(handle);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.OPEN_READ, o);
            Assert.assertEquals(segmentName, segment);
        });
        val openReadResult = s.openRead(segmentName).join();
        Assert.assertEquals(toReturn.get(), openReadResult);

        // OpenWrite
        toReturn.set(handle);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.OPEN_WRITE, o);
            Assert.assertEquals(segmentName, segment);
        });
        val openWriteResult = s.openWrite(segmentName).join();
        Assert.assertEquals(toReturn.get(), openWriteResult);

        // GetInfo
        toReturn.set(StreamSegmentInformation.builder().name(segmentName).build());
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.GET_INFO, o);
            Assert.assertEquals(segmentName, segment);
        });
        val getInfoResult = s.getStreamSegmentInfo(segmentName, TIMEOUT).join();
        Assert.assertEquals(toReturn.get(), getInfoResult);

        // Exists
        toReturn.set(true);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.EXISTS, o);
            Assert.assertEquals(segmentName, segment);
        });
        val existsResult = s.exists(segmentName, TIMEOUT).join();
        Assert.assertEquals(toReturn.get(), existsResult);

        // Read
        toReturn.set(10);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.READ, o);
            Assert.assertEquals(segmentName, segment);
        });
        val readResult = s.read(handle, 0, new byte[0], 0, 0, TIMEOUT).join();
        Assert.assertEquals(toReturn.get(), readResult);

        // Write
        toReturn.set(null);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.WRITE, o);
            Assert.assertEquals(segmentName, segment);
        });
        s.write(handle, 0, new ByteArrayInputStream(new byte[0]), 0, TIMEOUT).join();

        // Seal
        toReturn.set(null);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.SEAL, o);
            Assert.assertEquals(segmentName, segment);
        });
        s.seal(handle, TIMEOUT).join();

        // Concat
        toReturn.set(null);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.CONCAT, o);
            Assert.assertEquals(segmentName + "|" + concatSourceName, segment);
        });
        s.concat(handle, 0, concatSourceName, TIMEOUT).join();

        // Truncate
        toReturn.set(null);
        validator.set((o, segment) -> {
            Assert.assertEquals(TestStorage.TRUNCATE, o);
            Assert.assertEquals(segmentName, segment);
        });
        s.truncate(handle, 0, TIMEOUT).join();
    }

    @Test
    public void testConcurrency() {
        // TODO: write this
    }

    //region TestStorage

    @RequiredArgsConstructor
    private static class TestStorage implements SyncStorage {
        private static final String OPEN_READ = "openRead";
        private static final String GET_INFO = "getInfo";
        private static final String EXISTS = "exists";
        private static final String READ = "read";
        private static final String OPEN_WRITE = "openWrite";
        private static final String CREATE = "create";
        private static final String DELETE = "delete";
        private static final String WRITE = "write";
        private static final String SEAL = "seal";
        private static final String CONCAT = "concat";
        private static final String TRUNCATE = "truncate";
        private final BiFunction<String, String, Object> methodInvoked;

        @Override
        public SegmentHandle openRead(String streamSegmentName) throws StreamSegmentException {
            return (SegmentHandle) this.methodInvoked.apply(OPEN_READ, streamSegmentName);
        }

        @Override
        public int read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length) throws StreamSegmentException {
            return (Integer) this.methodInvoked.apply(READ, handle.getSegmentName());
        }

        @Override
        public SegmentProperties getStreamSegmentInfo(String streamSegmentName) throws StreamSegmentException {
            return (SegmentProperties) this.methodInvoked.apply(GET_INFO, streamSegmentName);
        }

        @Override
        public boolean exists(String streamSegmentName) {
            return (Boolean) this.methodInvoked.apply(EXISTS, streamSegmentName);
        }

        @Override
        public SegmentHandle openWrite(String streamSegmentName) throws StreamSegmentException {
            return (SegmentHandle) this.methodInvoked.apply(OPEN_WRITE, streamSegmentName);
        }

        @Override
        public SegmentProperties create(String streamSegmentName) throws StreamSegmentException {
            return (SegmentProperties) this.methodInvoked.apply(CREATE, streamSegmentName);
        }

        @Override
        public void delete(SegmentHandle handle) throws StreamSegmentException {
            this.methodInvoked.apply(DELETE, handle.getSegmentName());
        }

        @Override
        public void write(SegmentHandle handle, long offset, InputStream data, int length) throws StreamSegmentException {
            this.methodInvoked.apply(WRITE, handle.getSegmentName());
        }

        @Override
        public void seal(SegmentHandle handle) throws StreamSegmentException {
            this.methodInvoked.apply(SEAL, handle.getSegmentName());
        }

        @Override
        public void concat(SegmentHandle targetHandle, long offset, String sourceSegment) throws StreamSegmentException {
            this.methodInvoked.apply(CONCAT, targetHandle.getSegmentName() + "|" + sourceSegment);
        }

        @Override
        public void truncate(SegmentHandle handle, long offset) throws StreamSegmentException {
            this.methodInvoked.apply(TRUNCATE, handle.getSegmentName());
        }

        // region Unimplemented methods

        @Override
        public void unseal(SegmentHandle handle) throws StreamSegmentException {
        }

        @Override
        public boolean supportsTruncation() {
            return true;
        }

        @Override
        public void close() {
        }

        @Override
        public void initialize(long containerEpoch) {
        }

        //endregion
    }

    //endregion
}
