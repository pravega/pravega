/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.service.storage.mocks;

import io.pravega.common.Exceptions;
import io.pravega.service.contracts.SegmentProperties;
import io.pravega.service.storage.SegmentHandle;
import io.pravega.service.storage.StorageFactory;
import io.pravega.service.storage.TruncateableStorage;
import com.google.common.base.Preconditions;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.RequiredArgsConstructor;

/**
 * In-Memory mock for StorageFactory. Contents is destroyed when object is garbage collected.
 */
public class InMemoryStorageFactory implements StorageFactory, AutoCloseable {
    private final InMemoryStorage baseStorage;

    public InMemoryStorageFactory(ScheduledExecutorService executor) {
        this.baseStorage = new InMemoryStorage(executor);
        this.baseStorage.initialize(1); // InMemoryStorage does not use epochs.
    }

    @Override
    public TruncateableStorage createStorageAdapter() {
        return new FencedWrapper(this.baseStorage);
    }

    @Override
    public void close() {
        this.baseStorage.close();
    }

    //region FencedWrapper

    @RequiredArgsConstructor
    private static class FencedWrapper implements TruncateableStorage, ListenableStorage {
        private final InMemoryStorage baseStorage;
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public void initialize(long epoch) {
            Preconditions.checkArgument(epoch > 0, "epoch must be a positive number.");

            // InMemoryStorage does not use epochs.
        }

        @Override
        public void close() {
            // We purposefully do not close the base adapter, as that is shared between all instances of this class.
            this.closed.set(true);
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
    }

    //endregion
}
