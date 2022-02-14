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
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;

import java.io.InputStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class FlakyStorage implements Storage {
    final Storage inner;
    final ScheduledExecutorService executorService;
    final Duration duration;

    public FlakyStorage(Storage inner, ScheduledExecutorService executorService, Duration duration) {
        this.inner = inner;
        this.executorService = executorService;
        this.duration = duration;
    }

    @Override
    public void initialize(long containerEpoch) {
        inner.initialize(containerEpoch);
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.openRead(streamSegmentName));
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.read(handle, offset, buffer, bufferOffset, length, timeout));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.getStreamSegmentInfo(streamSegmentName, timeout));
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.exists(streamSegmentName, timeout));
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.openWrite(streamSegmentName));
    }

    @Override
    public CompletableFuture<SegmentHandle> create(String streamSegmentName, SegmentRollingPolicy rollingPolicy, Duration timeout) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.create(streamSegmentName, rollingPolicy, timeout));
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.write(handle, offset, data, length, timeout));
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.seal(handle, timeout));
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.concat(targetHandle, offset, sourceSegment, timeout));
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.delete(handle, timeout));
    }

    @Override
    public CompletableFuture<Void> truncate(SegmentHandle handle, long offset, Duration timeout) {
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.truncate(handle, offset, timeout));
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
        return Futures.delayedFuture(duration, executorService).
                thenComposeAsync(v -> inner.listSegments());
    }

    @Override
    public void close() {
    }
}
