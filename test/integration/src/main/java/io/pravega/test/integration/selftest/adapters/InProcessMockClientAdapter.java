/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.test.integration.selftest.adapters;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.test.integration.selftest.TestConfig;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Client-based adapter that targets an in-process Client with a Mock Controller and Mock StreamSegmentStore.
 */
class InProcessMockClientAdapter extends ClientAdapterBase {
    //region Members

    private static final String LISTENING_ADDRESS = "localhost";
    private PravegaConnectionListener listener;
    private MockStreamManager streamManager;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the InProcessMockClientAdapter class.
     *
     * @param testConfig    The TestConfig to use.
     * @param testExecutor  An Executor to use for test-related async operations.
     */
    InProcessMockClientAdapter(TestConfig testConfig, ScheduledExecutorService testExecutor) {
        super(testConfig, testExecutor);
    }

    //endregion

    //region ClientAdapterBase Implementation

    @Override
    protected void startUp() throws Exception {
        int segmentStorePort = this.testConfig.getSegmentStorePort(0);
        this.listener = new PravegaConnectionListener(false, segmentStorePort, getStreamSegmentStore());
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
    protected ClientFactory getClientFactory() {
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
        return feature == Feature.Create
                || feature == Feature.Append;
    }

    protected StreamSegmentStore getStreamSegmentStore() {
        return new MockStreamSegmentStore();
    }

    //endregion

    //region MockStreamSegmentStore

    private static class MockStreamSegmentStore implements StreamSegmentStore {
        private final Set<String> segments = Collections.synchronizedSet(new HashSet<>());

        @Override
        public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
            if (this.segments.add(streamSegmentName)) {
                return CompletableFuture.completedFuture(null);
            } else {
                return Futures.failedFuture(new StreamSegmentExistsException(streamSegmentName));
            }
        }

        @Override
        public CompletableFuture<Void> append(String streamSegmentName, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            if (this.segments.contains(streamSegmentName)) {
                return CompletableFuture.completedFuture(null);
            } else {
                return Futures.failedFuture(new StreamSegmentNotExistsException(streamSegmentName));
            }
        }

        @Override
        public CompletableFuture<Void> append(String streamSegmentName, long offset, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            return append(streamSegmentName, data, attributeUpdates, timeout);
        }

        @Override
        public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, boolean waitForPendingOps, Duration timeout) {
            if (this.segments.contains(streamSegmentName)) {
                return CompletableFuture.completedFuture(StreamSegmentInformation.builder().name(streamSegmentName).build());
            } else {
                return Futures.failedFuture(new StreamSegmentNotExistsException(streamSegmentName));
            }
        }

        @Override
        public CompletableFuture<Void> updateAttributes(String streamSegmentName, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            throw new UnsupportedOperationException("updateAttributes");
        }

        @Override
        public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
            throw new UnsupportedOperationException("updateAttributes");
        }

        @Override
        public CompletableFuture<String> createTransaction(String parentStreamSegmentName, UUID transactionId, Collection<AttributeUpdate> attributes, Duration timeout) {
            throw new UnsupportedOperationException("updateAttributes");
        }

        @Override
        public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
            throw new UnsupportedOperationException("updateAttributes");
        }

        @Override
        public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("updateAttributes");
        }

        @Override
        public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("updateAttributes");
        }

        @Override
        public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
            throw new UnsupportedOperationException("truncateStreamSegment");
        }
    }

    //endregion
}
