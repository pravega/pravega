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

package com.emc.pravega.service.server;

import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.mocks.InMemoryStorage;
import com.emc.pravega.testcommon.ErrorInjector;
import com.google.common.base.Preconditions;
import lombok.Setter;

import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Test Storage. Wraps around an existing Storage, and allows controlling behavior for each method, such as injecting
 * errors, simulating non-availability, etc.
 */
public class TestStorage implements Storage {
    private final InMemoryStorage wrappedStorage;
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
    private WriteInterceptor writeInterceptor;
    @Setter
    private SealInterceptor sealInterceptor;
    @Setter
    private ConcatInterceptor concatInterceptor;

    public TestStorage(InMemoryStorage wrappedStorage) {
        Preconditions.checkNotNull(wrappedStorage, "wrappedStorage");
        this.wrappedStorage = wrappedStorage;
    }

    @Override
    public void close() {
        this.wrappedStorage.close();
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Map<String, String> attributes, Duration timeout) {
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.createErrorInjector)
                            .thenCompose(v -> this.wrappedStorage.create(streamSegmentName, attributes, timeout));
    }

    @Override
    public CompletableFuture<Void> open(String streamSegmentName) {
        return this.wrappedStorage.open(streamSegmentName);
    }

    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
        ErrorInjector.throwSyncExceptionIfNeeded(this.writeSyncErrorInjector);
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.writeAsyncErrorInjector)
                            .thenCompose(v -> {
                                WriteInterceptor wi = this.writeInterceptor;
                                CompletableFuture<Void> result = null;
                                if (wi != null) {
                                    result = wi.apply(streamSegmentName, offset, data, length, this.wrappedStorage);
                                }

                                return result != null ? result : CompletableFuture.completedFuture(null);
                            })
                            .thenCompose(v -> this.wrappedStorage.write(streamSegmentName, offset, data, length, timeout));
    }

    @Override
    public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
        ErrorInjector.throwSyncExceptionIfNeeded(this.sealSyncErrorInjector);
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.sealAsyncErrorInjector)
                            .thenCompose(v -> {
                                SealInterceptor si = this.sealInterceptor;
                                CompletableFuture<Void> result = null;
                                if (si != null) {
                                    result = si.apply(streamSegmentName, this.wrappedStorage);
                                }

                                return result != null ? result : CompletableFuture.completedFuture(null);
                            }).thenCompose(v -> this.wrappedStorage.seal(streamSegmentName, timeout));
    }

    @Override
    public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset, String sourceStreamSegmentName, Duration timeout) {
        ErrorInjector.throwSyncExceptionIfNeeded(this.concatSyncErrorInjector);
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.concatAsyncErrorInjector)
                            .thenCompose(v -> {
                                ConcatInterceptor ci = this.concatInterceptor;
                                CompletableFuture<Void> result = null;
                                if (ci != null) {
                                    result = ci.apply(targetStreamSegmentName, offset, sourceStreamSegmentName, this.wrappedStorage);
                                }

                                return result != null ? result : CompletableFuture.completedFuture(null);
                            }).thenCompose(v -> this.wrappedStorage.concat(targetStreamSegmentName, offset, sourceStreamSegmentName, timeout));
    }

    @Override
    public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.deleteErrorInjector)
                            .thenCompose(v -> this.wrappedStorage.delete(streamSegmentName, timeout));
    }

    @Override
    public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        ErrorInjector.throwSyncExceptionIfNeeded(this.readSyncErrorInjector);
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.readAsyncErrorInjector)
                            .thenCompose(v -> this.wrappedStorage.read(streamSegmentName, offset, buffer, bufferOffset, length, timeout));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.getErrorInjector)
                            .thenCompose(v -> this.wrappedStorage.getStreamSegmentInfo(streamSegmentName, timeout));
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.existsErrorInjector)
                            .thenCompose(v -> this.wrappedStorage.exists(streamSegmentName, timeout));
    }

    public void append(String streamSegmentName, InputStream data, int length) {
        this.wrappedStorage.append(streamSegmentName, data, length);
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
}
