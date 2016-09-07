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

import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.mocks.InMemoryDurableDataLogFactory;
import com.emc.pravega.testcommon.ErrorInjector;
import com.google.common.base.Preconditions;
import lombok.Cleanup;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Test DurableDataLog. Wraps around an existing DurableDataLog, and allows controlling behavior for each method, such
 * as injecting errors, simulating non-availability, etc.
 */
public class TestDurableDataLog implements DurableDataLog {
    //region Members

    private final DurableDataLog wrappedLog;
    private ErrorInjector<Exception> appendSyncErrorInjector;
    private ErrorInjector<Exception> appendAsyncErrorInjector;
    private ErrorInjector<Exception> getReaderInitialErrorInjector;
    private ErrorInjector<Exception> readSyncErrorInjector;
    private Consumer<ReadItem> readInterceptor;
    private Consumer<Long> truncateCallback;

    //endregion

    //region Constructor

    private TestDurableDataLog(DurableDataLog wrappedLog) {
        Preconditions.checkNotNull(wrappedLog, "wrappedLog");
        this.wrappedLog = wrappedLog;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.wrappedLog.close();
    }

    //endregion

    //region DurableDataLog Implementation

    @Override
    public void initialize(Duration timeout) throws DurableDataLogException {
        this.wrappedLog.initialize(timeout);
    }

    @Override
    public CompletableFuture<Long> append(InputStream data, Duration timeout) {
        ErrorInjector.throwSyncExceptionIfNeeded(this.appendSyncErrorInjector);
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.appendAsyncErrorInjector)
                .thenCompose(v -> this.wrappedLog.append(data, timeout));
    }

    @Override
    public CompletableFuture<Boolean> truncate(long upToSequence, Duration timeout) {
        Consumer<Long> truncateCallback = this.truncateCallback;
        return this.wrappedLog
                .truncate(upToSequence, timeout)
                .thenApply(result -> {
                    if (result && truncateCallback != null) {
                        truncateCallback.accept(upToSequence);
                    }

                    return result;
                });
    }

    @Override
    public CloseableIterator<ReadItem, DurableDataLogException> getReader(long afterSequence) throws DurableDataLogException {
        ErrorInjector.throwSyncExceptionIfNeeded(this.getReaderInitialErrorInjector);
        return new CloseableIteratorWrapper(this.wrappedLog.getReader(afterSequence), this.readSyncErrorInjector, this.readInterceptor);
    }

    @Override
    public int getMaxAppendLength() {
        return this.wrappedLog.getMaxAppendLength();
    }

    @Override
    public long getLastAppendSequence() {
        return this.wrappedLog.getLastAppendSequence();
    }

    //endregion

    //region Test Helper Methods

    /**
     * Sets the Truncation callback, which will be called if a truncation actually happened.
     *
     * @param callback
     */
    public void setTruncateCallback(Consumer<Long> callback) {
        this.truncateCallback = callback;
    }

    /**
     * Sets the ErrorInjectors for append exceptions.
     *
     * @param syncInjector  An ErrorInjector to throw sync exceptions. If null, no sync exceptions will be thrown.
     * @param asyncInjector An ErrorInjector to throw async exceptions (wrapped in CompletableFutures). If null, no async
     *                      exceptions will be thrown (from this wrapper).
     */
    public void setAppendErrorInjectors(ErrorInjector<Exception> syncInjector, ErrorInjector<Exception> asyncInjector) {
        this.appendSyncErrorInjector = syncInjector;
        this.appendAsyncErrorInjector = asyncInjector;
    }

    /**
     * Sets the ErrorInjectors for the read operation.
     *
     * @param getReaderInjector An ErrorInjector to throw sync exceptions during calls to getReader. If null, no exceptions
     *                          will be thrown when calling getReader.
     * @param readErrorInjector An ErrorInjector to throw sync exceptions during calls to getNext() from the iterator
     *                          returned by getReader. If null, no sync exceptions will be thrown.
     */
    public void setReadErrorInjectors(ErrorInjector<Exception> getReaderInjector, ErrorInjector<Exception> readErrorInjector) {
        this.getReaderInitialErrorInjector = getReaderInjector;
        this.readSyncErrorInjector = readErrorInjector;
    }

    /**
     * Sets the Read Interceptor that will be called with every getNext() invocation from the iterator returned by getReader.
     *
     * @param interceptor
     */
    public void setReadInterceptor(Consumer<ReadItem> interceptor) {
        this.readInterceptor = interceptor;
    }

    /**
     * Retrieves all the entries from the DurableDataLog and converts them to the desired type.
     *
     * @param converter
     * @param <T>
     * @return
     * @throws DurableDataLogException
     */
    public <T> List<T> getAllEntries(FunctionWithException<ReadItem, T> converter) throws Exception {
        ArrayList<T> result = new ArrayList<>();
        @Cleanup
        CloseableIterator<ReadItem, DurableDataLogException> reader = this.wrappedLog.getReader(-1);
        while (true) {
            DurableDataLog.ReadItem readItem = reader.getNext();
            if (readItem == null) {
                break;
            }

            result.add(converter.apply(readItem));
        }

        return result;
    }

    //endregion

    //region Factory

    /**
     * Creates a new TestDurableDataLog backed by an InMemoryDurableDataLog.
     *
     * @param containerId   The Id of the container.
     * @param maxAppendSize The maximum append size for the log.
     * @return The newly created log.
     */
    public static TestDurableDataLog create(int containerId, int maxAppendSize) {
        try (InMemoryDurableDataLogFactory factory = new InMemoryDurableDataLogFactory(maxAppendSize)) {
            DurableDataLog log = factory.createDurableDataLog(containerId);
            return create(log);
        }
    }

    /**
     * Creates a new TestDurableDataLog wrapping the given one.
     *
     * @param wrappedLog
     * @return
     */
    public static TestDurableDataLog create(DurableDataLog wrappedLog) {
        return new TestDurableDataLog(wrappedLog);
    }

    //endregion

    public interface FunctionWithException<T, R> {
        R apply(T var1) throws Exception;
    }

    private static class CloseableIteratorWrapper implements CloseableIterator<ReadItem, DurableDataLogException> {
        private final CloseableIterator<ReadItem, DurableDataLogException> innerIterator;
        private final ErrorInjector<Exception> getNextErrorInjector;
        private final Consumer<ReadItem> readInterceptor;

        public CloseableIteratorWrapper(CloseableIterator<ReadItem, DurableDataLogException> innerIterator, ErrorInjector<Exception> getNextErrorInjector, Consumer<ReadItem> readInterceptor) {
            assert innerIterator != null;
            this.innerIterator = innerIterator;
            this.getNextErrorInjector = getNextErrorInjector;
            this.readInterceptor = readInterceptor;
        }

        @Override
        public ReadItem getNext() throws DurableDataLogException {
            ErrorInjector.throwSyncExceptionIfNeeded(getNextErrorInjector);
            ReadItem readItem = this.innerIterator.getNext();
            if (this.readInterceptor != null) {
                this.readInterceptor.accept(readItem);
            }

            return readItem;
        }

        @Override
        public void close() {
            this.innerIterator.close();
        }
    }
}
