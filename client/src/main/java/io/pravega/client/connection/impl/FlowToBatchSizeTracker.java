/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.connection.impl;

import io.pravega.common.util.SimpleCache;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
public class FlowToBatchSizeTracker {

    private static final int TRACKER_CACHE_MAX_SIZE = 100000;
    private static final Duration TRACKER_CACHE_EXPIRATION_TIME = Duration.ofSeconds(60);

    private final SimpleCache<Long, AppendBatchSizeTracker> flowToBatchSizeTrackerMap;

    public FlowToBatchSizeTracker() {
        flowToBatchSizeTrackerMap = new SimpleCache<>(TRACKER_CACHE_MAX_SIZE,
                TRACKER_CACHE_EXPIRATION_TIME, (flow, tracker) -> log.info("Evicting batch tracker for flow: {}", flow));
    }

    public AppendBatchSizeTracker getAppendBatchSizeTrackerByFlowId(long flowId) {
        if (flowToBatchSizeTrackerMap.putIfAbsent(flowId, new AppendBatchSizeTrackerImpl()) == null) {
            log.info("Instantiating new batch sze tracker for flow: {}", flowId);
        }
        return flowToBatchSizeTrackerMap.get(flowId);
    }

}
