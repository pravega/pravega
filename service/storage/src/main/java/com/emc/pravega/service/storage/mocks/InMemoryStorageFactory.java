/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.mocks;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.storage.SegmentHandle;
import com.emc.pravega.service.storage.StorageFactory;
import com.emc.pravega.service.storage.TruncateableStorage;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;

/**
 * In-Memory mock for StorageFactory. Contents is destroyed when object is garbage collected.
 */
public class InMemoryStorageFactory implements StorageFactory {
    private final InMemoryStorage baseStorage;

    public InMemoryStorageFactory(ScheduledExecutorService executor) {
        this.baseStorage = new InMemoryStorage(executor);
    }

    @Override
    public TruncateableStorage createStorageAdapter() {
        return new FencedWrapper(this.baseStorage);
    }

    //region FencedWrapper

    @RequiredArgsConstructor
    private static class FencedWrapper implements TruncateableStorage, ListenableStorage {
        private final InMemoryStorage baseStorage;
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public void initialize(long epoch) {
            this.baseStorage.initialize(epoch);
        }

        @Override
        public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            return this.baseStorage.openRead(streamSegmentName);
        }

        @Override
        public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            return this.baseStorage.read(handle, offset, buffer, bufferOffset, length, timeout);
        }

        @Override
        public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            return this.baseStorage.getStreamSegmentInfo(streamSegmentName, timeout);
        }

        @Override
        public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            return this.baseStorage.exists(streamSegmentName, timeout);
        }

        @Override
        public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            return this.baseStorage.openWrite(streamSegmentName);
        }

        @Override
        public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            return this.baseStorage.create(streamSegmentName, timeout);
        }

        @Override
        public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            return this.baseStorage.write(handle, offset, data, length, timeout);
        }

        @Override
        public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            return this.baseStorage.seal(handle, timeout);
        }

        @Override
        public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            return this.baseStorage.concat(targetHandle, offset, sourceSegment, timeout);
        }

        @Override
        public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            return this.baseStorage.delete(handle, timeout);
        }

        @Override
        public CompletableFuture<Void> truncate(String streamSegmentName, long offset, Duration timeout) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            return this.baseStorage.truncate(streamSegmentName, offset, timeout);
        }

        @Override
        public CompletableFuture<Void> registerSizeTrigger(String segmentName, long offset, Duration timeout) {
            return this.baseStorage.registerSizeTrigger(segmentName, offset, timeout);
        }

        @Override
        public CompletableFuture<Void> registerSealTrigger(String segmentName, Duration timeout) {
            return this.baseStorage.registerSealTrigger(segmentName, timeout);
        }

        @Override
        public void close() {
            this.closed.set(true);
        }
    }

    //endregion
}
