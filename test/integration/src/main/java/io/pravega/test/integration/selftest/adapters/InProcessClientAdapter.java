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
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.integration.selftest.TestConfig;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Client-based adapter that targets an in-process Pravega Cluster.
 */
public class InProcessClientAdapter extends ClientAdapterBase {
    private final SegmentStoreAdapter segmentStoreAdapter;
    private PravegaConnectionListener listener;
    private MockStreamManager streamManager;

    /**
     * Creates a new instance of the InProcessClientAdapter class.
     *
     * @param testConfig    The TestConfig to use.
     * @param builderConfig The ServiceBuilderConfig to use.
     * @param testExecutor  An Executor to use for test-related async operations.
     */
    public InProcessClientAdapter(TestConfig testConfig, ServiceBuilderConfig builderConfig, ScheduledExecutorService testExecutor) {
        super(testConfig, testExecutor);
        this.segmentStoreAdapter = new SegmentStoreAdapter(testConfig, builderConfig, testExecutor);
    }

    //region AutoCloseable Implementation

    @Override
    public void close() {
        super.close();

        if (this.listener != null) {
            this.listener.close();
            this.listener = null;
        }

        if (this.streamManager != null) {
            this.streamManager.close();
            this.streamManager = null;
        }

        this.segmentStoreAdapter.close();
    }

    @Override
    protected StreamManager getStreamManager() {
        return this.streamManager;
    }

    @Override
    protected ClientFactory getClientFactory() {
        return this.streamManager.getClientFactory();
    }

    //endregion

    @Override
    public boolean isFeatureSupported(Feature feature) {
        // This uses MockStreamManager, which only supports Create and Append.
        return feature == Feature.Create
                || feature == Feature.Append;
    }

    @Override
    public void initialize() throws Exception {
        this.segmentStoreAdapter.initialize();
        this.listener = new PravegaConnectionListener(false, getListeningPort(), this.segmentStoreAdapter.getStreamSegmentStore());
        this.listener.startListening();

        this.streamManager = new MockStreamManager(getScope(), getListeningAddress(), getListeningPort());
        this.streamManager.createScope(getScope());

        super.initialize();
    }

    @Override
    public ExecutorServiceHelpers.Snapshot getStorePoolSnapshot() {
        return this.segmentStoreAdapter.getStorePoolSnapshot();
    }
}
