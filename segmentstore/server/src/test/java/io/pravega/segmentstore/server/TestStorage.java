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
package io.pravega.segmentstore.server;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.test.common.ErrorInjector;
import java.io.InputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Setter;

/**
 * Test Storage. Wraps around an existing Storage, and allows controlling behavior for each method, such as injecting
 * errors, simulating non-availability, etc.
 */
@NotThreadSafe
public class TestStorage implements Storage {
    private final Storage wrappedStorage;
    private final HashMap<String, Long> truncationOffsets;
    private final InMemoryStorage wrappedSyncStorage;
    @Setter
    private ErrorInjector<Exception> writeSyncErrorInjector;
    @Setter
    private ErrorInjector<Exception> writeAsyncErrorInjector;
    @Setter
    private ErrorInjector<Exception> createErrorInjector;
    @Setter
    private ErrorInjector<Exception> deleteErrorInjector;
    @Setter
    private ErrorInjector<Exception> sealSyncErrorInjector;
    @Setter
    private ErrorInjector<Exception> sealAsyncErrorInjector;
    @Setter
    private ErrorInjector<Exception> concatSyncErrorInjector;
    @Setter
    private ErrorInjector<Exception> concatAsyncErrorInjector;
    @Setter
    private ErrorInjector<Exception> readSyncErrorInjector;
    @Setter
    private ErrorInjector<Exception> readAsyncErrorInjector;
    @Setter
    private ErrorInjector<Exception> getErrorInjector;
    @Setter
    private ErrorInjector<Exception> existsErrorInjector;
    @Setter
    private CreateInterceptor createInterceptor;
    @Setter
    private WriteInterceptor writeInterceptor;
    @Setter
    private SealInterceptor sealInterceptor;
    @Setter
    private ConcatInterceptor concatInterceptor;
    @Setter
    private TruncateInterceptor truncateInterceptor;
    @Setter
    private ReadInterceptor readInterceptor;

    public TestStorage(InMemoryStorage wrappedStorage, Executor executor) {
        Preconditions.checkNotNull(wrappedStorage, "wrappedStorage");
        this.wrappedSyncStorage = Preconditions.checkNotNull(wrappedStorage, "wrappedStorage");
        this.wrappedStorage = new AsyncStorageWrapper(wrappedStorage, executor);
        this.truncationOffsets = new HashMap<>();
    }

    @Override
    public void close() {
        this.wrappedStorage.close();
    }

    @Override
    public void initialize(long epoch) {
        // Nothing to do.
        this.wrappedStorage.initialize(epoch);
    }

    @Override
    public CompletableFuture<SegmentHandle> create(String streamSegmentName, Duration timeout) {
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.createErrorInjector,
                () -> this.wrappedStorage.create(streamSegmentName, timeout))
                            .thenApply(sp -> {
                                this.truncationOffsets.put(streamSegmentName, 0L);
                                return sp;
                            });
    }

    @Override
    public CompletableFuture<SegmentHandle> create(String streamSegmentName, SegmentRollingPolicy rollingPolicy, Duration timeout) {
        return ErrorInjector
                .throwAsyncExceptionIfNeeded(this.createErrorInjector, () -> {
                    CreateInterceptor ci = this.createInterceptor;
                    CompletableFuture<Void> result = null;
                    if (ci != null) {
                        result = ci.apply(streamSegmentName, rollingPolicy, this.wrappedStorage);
                    }

                    return result != null ? result : CompletableFuture.completedFuture(null);
                })
                .thenCompose(v -> this.wrappedStorage.create(streamSegmentName, rollingPolicy, timeout))
                .thenApply(sp -> {
                    this.truncationOffsets.put(streamSegmentName, 0L);
                    return sp;
                });
    }

    @Override
    public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
        return this.wrappedStorage.openRead(streamSegmentName);
    }

    @Override
    public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
        return this.wrappedStorage.openWrite(streamSegmentName);
    }

    @Override
    public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
        ErrorInjector.throwSyncExceptionIfNeeded(this.writeSyncErrorInjector);
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.writeAsyncErrorInjector, () -> {
                                WriteInterceptor wi = this.writeInterceptor;
                                CompletableFuture<Void> result = null;
                                if (wi != null) {
                                    result = wi.apply(handle.getSegmentName(), offset, data, length, this.wrappedStorage);
                                }

                                return result != null ? result : CompletableFuture.completedFuture(null);
                            })
                            .thenCompose(v -> this.wrappedStorage.write(handle, offset, data, length, timeout));
    }

    @Override
    public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
        ErrorInjector.throwSyncExceptionIfNeeded(this.sealSyncErrorInjector);
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.sealAsyncErrorInjector, () -> {
                                SealInterceptor si = this.sealInterceptor;
                                CompletableFuture<Void> result = null;
                                if (si != null) {
                                    result = si.apply(handle.getSegmentName(), this.wrappedStorage);
                                }

                                return result != null ? result : CompletableFuture.completedFuture(null);
                            }).thenCompose(v -> this.wrappedStorage.seal(handle, timeout));
    }

    @Override
    public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
        ErrorInjector.throwSyncExceptionIfNeeded(this.concatSyncErrorInjector);
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.concatAsyncErrorInjector, () -> {
                                ConcatInterceptor ci = this.concatInterceptor;
                                CompletableFuture<Void> result = null;
                                if (ci != null) {
                                    result = ci.apply(targetHandle.getSegmentName(), offset, sourceSegment, this.wrappedStorage);
                                }

                                return result != null ? result : CompletableFuture.completedFuture(null);
                            }).thenCompose(v -> this.wrappedStorage.concat(targetHandle, offset, sourceSegment, timeout));
    }

    @Override
    public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.deleteErrorInjector,
                () -> this.wrappedStorage.delete(handle, timeout))
                            .thenRun(() -> this.truncationOffsets.remove(handle.getSegmentName()));
    }

    @Override
    public CompletableFuture<Void> truncate(SegmentHandle handle, long offset, Duration timeout) {
        TruncateInterceptor ti = this.truncateInterceptor;
        CompletableFuture<Void> result;
        if (ti != null) {
            result = ti.apply(handle.getSegmentName(), offset, this.wrappedStorage);
        } else {
            result = CompletableFuture.completedFuture(null);
        }

        return result.thenRun(() -> truncateDirectly(handle, offset));
    }

    @Override
    public boolean supportsTruncation() {
        return true;
    }

    @Override
    public boolean supportsAtomicWrites() {
        return this.wrappedStorage.supportsAtomicWrites();
    }

    @Override
    public CompletableFuture<Iterator<SegmentProperties>> listSegments() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        ReadInterceptor ri = this.readInterceptor;
        if (ri != null) {
            ri.accept(handle.getSegmentName(), this.wrappedStorage);
        }

        ErrorInjector.throwSyncExceptionIfNeeded(this.readSyncErrorInjector);
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.readAsyncErrorInjector,
                () -> this.wrappedStorage.read(handle, offset, buffer, bufferOffset, length, timeout));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.getErrorInjector,
                () -> this.wrappedStorage.getStreamSegmentInfo(streamSegmentName, timeout));
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.existsErrorInjector,
                () -> this.wrappedStorage.exists(streamSegmentName, timeout));
    }

    public void append(SegmentHandle handle, InputStream data, int length) {
        this.wrappedSyncStorage.append(handle, data, length);
    }

    public void truncateDirectly(SegmentHandle handle, long offset) {
        if (!this.truncationOffsets.containsKey(handle.getSegmentName())) {
            throw new CompletionException(new StreamSegmentNotExistsException(handle.getSegmentName()));
        }

        this.truncationOffsets.put(handle.getSegmentName(), offset);
    }

    public long getTruncationOffset(String streamSegmentName) {
        return this.truncationOffsets.get(streamSegmentName);
    }

    @FunctionalInterface
    public interface CreateInterceptor {
        CompletableFuture<Void> apply(String streamSegmentName, SegmentRollingPolicy rollingPolicy, Storage wrappedStorage);
    }

    @FunctionalInterface
    public interface WriteInterceptor {
        CompletableFuture<Void> apply(String streamSegmentName, long offset, InputStream data, int length, Storage wrappedStorage);
    }

    @FunctionalInterface
    public interface SealInterceptor {
        CompletableFuture<Void> apply(String streamSegmentName, Storage wrappedStorage);
    }

    @FunctionalInterface
    public interface ConcatInterceptor {
        CompletableFuture<Void> apply(String targetStreamSegmentName, long offset, String sourceStreamSegmentName, Storage wrappedStorage);
    }

    @FunctionalInterface
    public interface TruncateInterceptor {
        CompletableFuture<Void> apply(String streamSegmentName, long truncateOffset, Storage wrappedStorage);
    }

    @FunctionalInterface
    public interface ReadInterceptor {
        void accept(String streamSegmentName, Storage wrappedStorage);
    }
}
