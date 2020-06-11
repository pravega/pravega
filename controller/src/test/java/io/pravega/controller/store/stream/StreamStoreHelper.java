/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import org.apache.curator.framework.CuratorFramework;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

public class StreamStoreHelper {

    public static StreamMetadataStore getPravegaTablesStreamStore(SegmentHelper segHelper,
                                                                  CuratorFramework cli,
                                                                  ScheduledExecutorService executor,
                                                                  Duration gcPeriod, GrpcAuthHelper authHelper) {
        return new PravegaTablesStreamMetadataStore(segHelper, cli, executor, gcPeriod, authHelper);
    }

    public static StreamMetadataStore getZKStreamStore(CuratorFramework cli,
                                                       ScheduledExecutorService executor) {
        return new ZKStreamMetadataStore(cli, executor);
    }
}
