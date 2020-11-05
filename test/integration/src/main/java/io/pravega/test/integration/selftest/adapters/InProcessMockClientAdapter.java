/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.test.integration.selftest.adapters;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.MergeStreamSegmentResult;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.test.common.NoOpScheduledExecutor;
import io.pravega.test.integration.selftest.TestConfig;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.concurrent.GuardedBy;
import lombok.val;

/**
 * Client-based adapter that targets an in-process Client with a Mock Controller and Mock StreamSegmentStore.
 */
class InProcessMockClientAdapter extends ClientAdapterBase {
    //region Members

    private static final String LISTENING_ADDRESS = "localhost";
    private final ScheduledExecutorService executor;
    private PravegaConnectionListener listener;
    private MockStreamManager streamManager;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the InProcessMockClientAdapter class.
     *
     * @param testConfig   The TestConfig to use.
     * @param testExecutor An Executor to use for test-related async operations.
     */
    InProcessMockClientAdapter(TestConfig testConfig, ScheduledExecutorService testExecutor) {
        super(testConfig, testExecutor);
        this.executor = testExecutor;
    }

    //endregion

    //region ClientAdapterBase Implementation

    @Override
    protected void startUp() throws Exception {
        int segmentStorePort = this.testConfig.getSegmentStorePort(0);
        this.listener = new PravegaConnectionListener(false, segmentStorePort, getStreamSegmentStore(),
                getTableStore(), NoOpScheduledExecutor.get());
        this.listener.startListening();

        this.streamManager = new MockStreamManager(SCOPE, LISTENING_ADDRESS, segmentStorePort);
        this.streamManager.createScope(SCOPE);
        super.startUp();
    }

    @Override
    protected void shutDown() {
        super.shutDown();

        if (this.listener != null) {
            this.listener.close();
            this.listener = null;
        }

        if (this.streamManager != null) {
            this.streamManager.close();
            this.streamManager = null;
        }
    }

    @Override
    protected StreamManager getStreamManager() {
        return this.streamManager;
    }

    @Override
    protected EventStreamClientFactory getClientFactory() {
        return this.streamManager.getClientFactory();
    }

    @Override
    protected String getControllerUrl() {
        throw new UnsupportedOperationException("getControllerUrl is not supported for Mock implementations.");
    }

    @Override
    public boolean isFeatureSupported(Feature feature) {
        // This uses MockStreamManager, which only supports Create and Append.
        // Also the MockStreamSegmentStore does not support any other features as well.
        return feature == Feature.CreateStream
                || feature == Feature.Append;
    }

    protected StreamSegmentStore getStreamSegmentStore() {
        return new MockStreamSegmentStore();
    }

    protected TableStore getTableStore() {
        return new MockTableStore();
    }

    //endregion

    //region MockStreamSegmentStore

    private class MockStreamSegmentStore implements StreamSegmentStore {
        @GuardedBy("lock")
        private final Map<String, Long> segments = new HashMap<>();
        @GuardedBy("lock")
        private final Map<String, Map<UUID, Long>> attributes = new HashMap<>();
        private final Object lock = new Object();

        @Override
        public CompletableFuture<Void> createStreamSegment(String streamSegmentName, SegmentType segmentType,
                                                           Collection<AttributeUpdate> attributes, Duration timeout) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this.lock) {
                    if (this.segments.put(streamSegmentName, 0L) == null) {
                        this.attributes.put(streamSegmentName, new ConcurrentHashMap<>());
                    } else {
                        throw new CompletionException(new StreamSegmentExistsException(streamSegmentName));
                    }
                }
            }, executor);
        }

        @Override
        public CompletableFuture<Long> append(String streamSegmentName, BufferView data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.lock) {
                    long offset = this.segments.getOrDefault(streamSegmentName, -1L);
                    if (offset >= 0) {
                        if (attributeUpdates != null) {
                            val segmentAttributes = this.attributes.get(streamSegmentName);
                            attributeUpdates.forEach(au -> segmentAttributes.put(au.getAttributeId(), au.getValue()));
                        }
                        this.segments.put(streamSegmentName, offset + data.getLength());
                        return offset;
                    } else {
                        throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
                    }
                }
            }, executor);
        }

        @Override
        public CompletableFuture<Long> append(String streamSegmentName, long offset, BufferView data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            return append(streamSegmentName, data, attributeUpdates, timeout);
        }

        @Override
        public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.lock) {
                    long length = this.segments.getOrDefault(streamSegmentName, -1L);
                    if (length >= 0) {
                        return StreamSegmentInformation.builder().name(streamSegmentName)
                                .length(length)
                                .attributes(new HashMap<>(this.attributes.get(streamSegmentName))).build();
                    } else {
                        throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
                    }
                }
            }, executor);
        }

        @Override
        public CompletableFuture<Void> updateAttributes(String streamSegmentName, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this.lock) {
                    val segmentAttributes = this.attributes.get(streamSegmentName);
                    if (attributeUpdates != null) {
                        attributeUpdates.forEach(au -> segmentAttributes.put(au.getAttributeId(), au.getValue()));
                    } else {
                        throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
                    }
                }
            }, executor);
        }

        @Override
        public CompletableFuture<Map<UUID, Long>> getAttributes(String streamSegmentName, Collection<UUID> attributeIds, boolean cache, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.lock) {
                    val segmentAttributes = this.attributes.get(streamSegmentName);
                    if (segmentAttributes != null) {
                        return new HashMap<>(segmentAttributes);
                    } else {
                        throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
                    }
                }
            }, executor);
        }

        @Override
        public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
            throw new UnsupportedOperationException("read");
        }

        @Override
        public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String target, String source, Duration timeout) {
            throw new UnsupportedOperationException("mergeStreamSegment");
        }

        @Override
        public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("sealStreamSegment");
        }

        @Override
        public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("deleteStreamSegment");
        }

        @Override
        public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
            throw new UnsupportedOperationException("truncateStreamSegment");
        }
    }

    //endregion

    private static class MockTableStore implements TableStore {
        @Override
        public CompletableFuture<Void> createSegment(String segmentName, SegmentType segmentType, Duration timeout) {
            throw new UnsupportedOperationException("createTableSegment");
        }

        @Override
        public CompletableFuture<Void> deleteSegment(String segmentName, boolean mustBeEmpty, Duration timeout) {
            throw new UnsupportedOperationException("deleteTableSegment");
        }

        @Override
        public CompletableFuture<Void> merge(String targetSegmentName, String sourceSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("mergeTableSegments");
        }

        @Override
        public CompletableFuture<Void> seal(String segmentName, Duration timeout) {
            throw new UnsupportedOperationException("sealTableSegment");
        }

        @Override
        public CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, Duration timeout) {
            throw new UnsupportedOperationException("updateTableSegment");
        }

        @Override
        public CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, long tableSegmentOffset, Duration timeout) {
            throw new UnsupportedOperationException("updateTableSegment");
        }

        @Override
        public CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, Duration timeout) {
            throw new UnsupportedOperationException("remove");
        }

        @Override
        public CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, long tableSegmentOffset, Duration timeout) {
            throw new UnsupportedOperationException("remove");
        }

        @Override
        public CompletableFuture<List<TableEntry>> get(String segmentName, List<BufferView> keys, Duration timeout) {
            throw new UnsupportedOperationException("get");
        }

        @Override
        public CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(String segmentName, IteratorArgs args) {
            throw new UnsupportedOperationException("keyIterator");
        }

        @Override
        public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(String segmentName, IteratorArgs args) {
            throw new UnsupportedOperationException("entryIterator");
        }

        @Override
        public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryDeltaIterator(String segmentName, long fromPosition, Duration fetchTimeout) {
            throw new UnsupportedOperationException("entryIterator");
        }
    }
}
