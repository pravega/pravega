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

    @Override
    public void close() {
        super.close();

        if (this.listener != null) {
            this.listener.close();
            this.listener = null;
        }

        this.segmentStoreAdapter.close();
    }

    @Override
    public void initialize() throws Exception {
        this.segmentStoreAdapter.initialize();
        this.listener = new PravegaConnectionListener(false, this.listeningPort, this.segmentStoreAdapter.getStreamSegmentStore());
        this.listener.startListening();

        super.initialize();
    }

    @Override
    public ExecutorServiceHelpers.Snapshot getStorePoolSnapshot() {
        return this.segmentStoreAdapter.getStorePoolSnapshot();
    }
}
