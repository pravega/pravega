/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.server.TableStoreMock;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ErrorInjector;
import io.pravega.test.common.IntentionalException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.Getter;
import lombok.SneakyThrows;

/**
 * Unit tests for the {@link TableMetadataStore} class.
 */
public class TableMetadataStoreTests extends MetadataStoreTestBase {
    @Override
    public void testCreateSegmentWithFailures() {
        final String segmentName = "NewSegment";

        @Cleanup
        TableTestContext context = (TableTestContext) createTestContext();
        context.tableStore.setPutErrorInjector(new ErrorInjector<>(i -> true, IntentionalException::new));
        AssertExtensions.assertSuppliedFutureThrows(
                "createSegment did not fail when random exception was thrown.",
                () -> context.getMetadataStore().createSegment(segmentName, null, TIMEOUT),
                ex -> ex instanceof IntentionalException);
    }

    @Override
    protected TestContext createTestContext(TestConnector connector) {
        TableTestContext context = new TableTestContext(connector);
        context.initialize();
        return context;
    }

    private class TableTestContext extends TestContext {
        final TestTableStore tableStore;
        @Getter
        final TableMetadataStore metadataStore;
        final AtomicInteger storageReadCount;

        TableTestContext(TestConnector connector) {
            super(connector);
            this.tableStore = new TestTableStore(executorService());
            this.metadataStore = new TableMetadataStore(this.connector, this.tableStore, executorService());
            this.storageReadCount = new AtomicInteger(0);
        }

        @SneakyThrows
        void initialize() {
            this.metadataStore.initialize(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        int getStoreReadCount() {
            return this.tableStore.getGetCount();
        }

        @Override
        void setGetInfoErrorInjector(ErrorInjector<Exception> ei) {
            this.tableStore.setGetErrorInjector(ei);
        }

        @Override
        public void close() {
            this.tableStore.close();
        }

        private class TestTableStore extends TableStoreMock {
            private final AtomicInteger getCount = new AtomicInteger();
            private final AtomicReference<ErrorInjector<Exception>> putErrorInjector = new AtomicReference<>();
            private final AtomicReference<ErrorInjector<Exception>> getErrorInjector = new AtomicReference<>();

            TestTableStore(Executor executor) {
                super(executor);
            }

            int getGetCount() {
                return this.getCount.get();
            }

            void setPutErrorInjector(ErrorInjector<Exception> ei) {
                this.putErrorInjector.set(ei);
            }

            void setGetErrorInjector(ErrorInjector<Exception> ei) {
                this.getErrorInjector.set(ei);
            }

            @Override
            public CompletableFuture<Void> createSegment(String segmentName, Duration timeout) {
                return metadataStore.createSegment(segmentName, null, timeout)
                                    .thenCompose(v -> super.createSegment(segmentName, timeout));
            }

            @Override
            public CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, Duration timeout) {
                return ErrorInjector.throwAsyncExceptionIfNeeded(
                        this.putErrorInjector.get(),
                        () -> super.put(segmentName, entries, timeout));
            }

            @Override
            public CompletableFuture<List<TableEntry>> get(String segmentName, List<ArrayView> keys, Duration timeout) {
                return ErrorInjector.throwAsyncExceptionIfNeeded(
                        this.getErrorInjector.get(),
                        () -> super.get(segmentName, keys, timeout)
                                   .thenApply(result -> {
                                       this.getCount.incrementAndGet();
                                       return result;
                                   }));
            }
        }
    }
}
