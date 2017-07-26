/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest.adapters;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.integration.selftest.TestConfig;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Client-based adapter that targets an in-process Client with a Mock Controller and a real StreamSegmentStore.
 */
public class InProcessListenerWithRealStoreAdapter extends InProcessMockClientAdapter {
    private final SegmentStoreAdapter segmentStoreAdapter;

    //region Constructor

    /**
     * Creates a new instance of the InProcessMockClientAdapter class.
     *
     * @param testConfig    The TestConfig to use.
     * @param builderConfig The ServiceBuilderConfig to use.
     * @param testExecutor  An Executor to use for test-related async operations.
     */
    public InProcessListenerWithRealStoreAdapter(TestConfig testConfig, ServiceBuilderConfig builderConfig, ScheduledExecutorService testExecutor) {
        super(testConfig, testExecutor);
        this.segmentStoreAdapter = new SegmentStoreAdapter(testConfig, builderConfig, testExecutor);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        super.close();
        this.segmentStoreAdapter.close();
    }

    //endregion

    //region InProcessMockClientAdapter Overrides

    @Override
    public void initialize() throws Exception {
        this.segmentStoreAdapter.initialize();
        super.initialize();
    }

    @Override
    public ExecutorServiceHelpers.Snapshot getStorePoolSnapshot() {
        return this.segmentStoreAdapter.getStorePoolSnapshot();
    }

    @Override
    protected StreamSegmentStore getStreamSegmentStore() {
        return this.segmentStoreAdapter.getStreamSegmentStore();
    }

    //endregion
}
