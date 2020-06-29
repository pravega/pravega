/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest.adapters;
/*
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.integration.selftest.TestConfig;
import java.util.concurrent.ScheduledExecutorService;

class InProcessListenerWithRealStoreAdapter extends InProcessMockClientAdapter {
    private final SegmentStoreAdapter segmentStoreAdapter;

    //region Constructor

    InProcessListenerWithRealStoreAdapter(TestConfig testConfig, ServiceBuilderConfig builderConfig, ScheduledExecutorService testExecutor) {
        super(testConfig, testExecutor);
        this.segmentStoreAdapter = new SegmentStoreAdapter(testConfig, builderConfig, testExecutor);
    }

    //endregion

    //region InProcessMockClientAdapter Overrides

    @Override
    protected void startUp() throws Exception {
        this.segmentStoreAdapter.startUp();
        super.startUp();
    }

    @Override
    protected void shutDown() {
        super.shutDown();
        this.segmentStoreAdapter.shutDown();
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
*/
