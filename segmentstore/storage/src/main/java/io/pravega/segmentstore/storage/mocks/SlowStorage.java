/**
 * Copyright Pravega Authors.
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
package io.pravega.segmentstore.storage.mocks;

import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.storage.StorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.noop.StorageExtraConfig;
import lombok.Getter;

import java.io.InputStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

/**
 * {@link Storage} implementation for introducing delays/slowness to inner Storage instance.
 */
public class SlowStorage implements StorageWrapper {
    @Getter
    final protected Storage inner;

    @Getter
    final protected ScheduledExecutorService executorService;

    @Getter
    final protected Supplier<Duration> durationSupplier;

    /**
     * Creates a new instance of SlowStorage.
     * @param inner inner Storage for this instance.
     * @param executorService executorService to be used.
     * @param config configuration property used for that instance.
     */
    public SlowStorage(Storage inner, ScheduledExecutorService executorService, StorageExtraConfig config) {
        this.inner = inner;
        this.executorService = executorService;
        this.durationSupplier = SlowDelaySuppliers.getDurationSupplier(config);
    }

    @Override
    public void initialize(long containerEpoch) {
        inner.initialize(containerEpoch);
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        return Futures.delayedFuture(durationSupplier.get(), executorService).
                thenComposeAsync(v -> inner.openRead(streamSegmentName), executorService);
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return Futures.delayedFuture(durationSupplier.get(), executorService).
                thenComposeAsync(v -> inner.read(handle, offset, buffer, bufferOffset, length, timeout), executorService);
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return Futures.delayedFuture(durationSupplier.get(), executorService).
                thenComposeAsync(v -> inner.getStreamSegmentInfo(streamSegmentName, timeout), executorService);
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return Futures.delayedFuture(durationSupplier.get(), executorService).
                thenComposeAsync(v -> inner.exists(streamSegmentName, timeout), executorService);
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        return Futures.delayedFuture(durationSupplier.get(), executorService).
                thenComposeAsync(v -> inner.openWrite(streamSegmentName), executorService);
    }

    @Override
    public CompletableFuture<SegmentHandle> create(String streamSegmentName, SegmentRollingPolicy rollingPolicy, Duration timeout) {
        return Futures.delayedFuture(durationSupplier.get(), executorService).
                thenComposeAsync(v -> inner.create(streamSegmentName, rollingPolicy, timeout), executorService);
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        return Futures.delayedFuture(durationSupplier.get(), executorService).
                thenComposeAsync(v -> inner.write(handle, offset, data, length, timeout), executorService);
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        return Futures.delayedFuture(durationSupplier.get(), executorService).
                thenComposeAsync(v -> inner.seal(handle, timeout), executorService);
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        return Futures.delayedFuture(durationSupplier.get(), executorService).
                thenComposeAsync(v -> inner.concat(targetHandle, offset, sourceSegment, timeout), executorService);
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        return Futures.delayedFuture(durationSupplier.get(), executorService).
                thenComposeAsync(v -> inner.delete(handle, timeout), executorService);
    }

    @Override
    public CompletableFuture<Void> truncate(SegmentHandle handle, long offset, Duration timeout) {
        return Futures.delayedFuture(durationSupplier.get(), executorService).
                thenComposeAsync(v -> inner.truncate(handle, offset, timeout), executorService);
    }

    @Override
    public boolean supportsTruncation() {
        return inner.supportsTruncation();
    }

    @Override
    public boolean supportsAtomicWrites() {
        return inner.supportsAtomicWrites();
    }

    @Override
    public CompletableFuture<Iterator<SegmentProperties>> listSegments() {
        return Futures.delayedFuture(durationSupplier.get(), executorService).
                thenComposeAsync(v -> inner.listSegments(), executorService);
    }

    @Override
    public void close() {
        if (inner != null) {
            inner.close();
        }
    }

}
