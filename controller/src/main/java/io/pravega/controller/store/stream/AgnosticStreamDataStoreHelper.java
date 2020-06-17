/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.controller.server.SegmentHelper;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.Executor;

import org.apache.curator.framework.CuratorFramework;

@Slf4j
public class AgnosticStreamDataStoreHelper {
    @Getter(AccessLevel.PACKAGE)
    private final CuratorFramework client;
    private final Executor executor;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final SegmentHelper segmentHelper;

    public AgnosticStreamDataStoreHelper(final SegmentHelper segmentHelper, final CuratorFramework cf, Executor executor) {
        client = cf;
        this.segmentHelper = segmentHelper;
        this.executor = executor;
    }
}
