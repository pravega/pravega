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
import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.CompositeArrayView;
import io.pravega.segmentstore.storage.DataLogInitializationException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.segmentstore.storage.ReadOnlyLogMetadata;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.segmentstore.storage.WriteSettings;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.test.common.ErrorInjector;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.Cleanup;

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
    private Function<ReadItem, ReadItem> readInterceptor;
    private Consumer<LogAddress> truncateCallback;

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
    public void enable() throws DurableDataLogException {
        this.wrappedLog.enable();
    }

    @Override
    public void disable() throws DurableDataLogException {
        this.wrappedLog.disable();
    }

    @Override
    public CompletableFuture<LogAddress> append(CompositeArrayView data, Duration timeout) {
        ErrorInjector.throwSyncExceptionIfNeeded(this.appendSyncErrorInjector);
        return ErrorInjector.throwAsyncExceptionIfNeeded(this.appendAsyncErrorInjector,
                () -> this.wrappedLog.append(data, timeout));
    }

    @Override
    public CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout) {
        Consumer<LogAddress> truncateCallback = this.truncateCallback;
        return this.wrappedLog
                .truncate(upToAddress, timeout)
                .thenRun(() -> {
                    if (truncateCallback != null) {
                        truncateCallback.accept(upToAddress);
                    }
                });
    }

    @Override
    public CloseableIterator<ReadItem, DurableDataLogException> getReader() throws DurableDataLogException {
        ErrorInjector.throwSyncExceptionIfNeeded(this.getReaderInitialErrorInjector);
        return new CloseableIteratorWrapper(this.wrappedLog.getReader(), this.readSyncErrorInjector, this.readInterceptor);
    }

    @Override
    public WriteSettings getWriteSettings() {
        return this.wrappedLog.getWriteSettings();
    }

    @Override
    public ReadOnlyLogMetadata loadMetadata() throws DataLogInitializationException {
        throw new DataLogInitializationException("Unsupported Operation");
    }

    @Override
    public long getEpoch() {
        return this.wrappedLog.getEpoch();
    }

    @Override
    public void overrideEpoch(long epoch) throws DurableDataLogException {
        throw new DataLogInitializationException("Unsupported Operation");
    }

    @Override
    public QueueStats getQueueStatistics() {
        return this.wrappedLog.getQueueStatistics();
    }

    @Override
    public void registerQueueStateChangeListener(ThrottleSourceListener listener) {
        this.wrappedLog.registerQueueStateChangeListener(listener);
    }

    //endregion

    //region Test Helper Methods

    /**
     * Sets the Truncation callback, which will be called if a truncation actually happened.
     *
     * @param callback The callback to set.
     */
    public void setTruncateCallback(Consumer<LogAddress> callback) {
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
     * @param interceptor The read interceptor to set.
     */
    public void setReadInterceptor(Function<ReadItem, ReadItem> interceptor) {
        this.readInterceptor = interceptor;
    }

    /**
     * Retrieves all the entries from the DurableDataLog and converts them to the desired type.
     *
     * @param converter The converter to apply to each entry.
     * @param <T>       The resulting type of each entry's conversion.
     * @throws Exception If a general exception occurred.
     */
    public <T> List<T> getAllEntries(FunctionWithException<ReadItem, T> converter) throws Exception {
        ArrayList<T> result = new ArrayList<>();
        @Cleanup
        CloseableIterator<ReadItem, DurableDataLogException> reader = this.wrappedLog.getReader();
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
     * @param containerId     The Id of the container.
     * @param maxAppendSize   The maximum append size for the log.
     * @param executorService An executor to use for async operations.
     * @return The newly created log.
     */
    public static TestDurableDataLog create(int containerId, int maxAppendSize, ScheduledExecutorService executorService) {
        return create(containerId, maxAppendSize, 0, executorService);
    }

    /**
     * Creates a new TestDurableDataLog backed by an InMemoryDurableDataLog.
     *
     * @param containerId       The Id of the container.
     * @param maxAppendSize     The maximum append size for the log.
     * @param appendDelayMillis The amount of delay, in milliseconds, for each append operation.
     * @param executorService   An executor to use for async operations.
     * @return The newly created log.
     */
    public static TestDurableDataLog create(int containerId, int maxAppendSize, int appendDelayMillis, ScheduledExecutorService executorService) {
        try (InMemoryDurableDataLogFactory factory = new InMemoryDurableDataLogFactory(maxAppendSize, executorService)) {
            if (appendDelayMillis > 0) {
                Duration delay = Duration.ofMillis(appendDelayMillis);
                factory.setAppendDelayProvider(() -> delay);
            }

            DurableDataLog log = factory.createDurableDataLog(containerId);
            return create(log);
        }
    }

    /**
     * Creates a new TestDurableDataLog wrapping the given one.
     *
     * @param wrappedLog The DurableDataLog to wrap.
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
        private final Function<ReadItem, ReadItem> readInterceptor;

        CloseableIteratorWrapper(CloseableIterator<ReadItem, DurableDataLogException> innerIterator, ErrorInjector<Exception> getNextErrorInjector, Function<ReadItem, ReadItem> readInterceptor) {
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
                return this.readInterceptor.apply(readItem);
            }

            return readItem;
        }

        @Override
        public void close() {
            this.innerIterator.close();
        }
    }
}
