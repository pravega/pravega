/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.host;

import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

@Slf4j
public class HostStoreFactory {

    public static HostControllerStore createStore(final HostMonitorConfig hostMonitorConfig,
                                                  final StoreClient storeClient) {

        Preconditions.checkNotNull(hostMonitorConfig, "hostMonitorConfig");
        Preconditions.checkNotNull(storeClient, "storeClient");

        if (hostMonitorConfig.isHostMonitorEnabled()) {
            Preconditions.checkArgument(storeClient.getType() == StoreType.Zookeeper,
                    "If host monitor is enabled then the store type should be Zookeeper");
            log.info("Creating Zookeeper based host store");
            return new ZKHostStore((CuratorFramework) storeClient.getClient(), hostMonitorConfig.getContainerCount());
        } else {
            // We create an in-memory host store using the configuration passed in hostMonitorConfig.
            log.info("Creating in-memory host store");
            return createInMemoryStore(hostMonitorConfig);
        }
    }

    @VisibleForTesting
    public static HostControllerStore createInMemoryStore(HostMonitorConfig hostMonitorConfig) {
        log.info("Creating in-memory host store");
        return new InMemoryHostStore(hostMonitorConfig.getHostContainerMap(), hostMonitorConfig.getContainerCount());
    }
}
