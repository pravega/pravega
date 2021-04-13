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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.util.Config;
import org.apache.curator.framework.CuratorFramework;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

public class TestStreamStoreFactory {
    @VisibleForTesting
    public static StreamMetadataStore createPravegaTablesStore(final CuratorFramework client,
                                                               final ScheduledExecutorService executor, PravegaTablesStoreHelper helper) {
        return new PravegaTablesStreamMetadataStore(client, executor, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS), helper);
    }
}
