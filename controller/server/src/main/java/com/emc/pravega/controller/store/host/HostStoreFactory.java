/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.host;

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.controller.store.client.StoreClient;
import com.emc.pravega.controller.store.client.StoreType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
            return new ZKHostStore((CuratorFramework) storeClient.getClient());
        } else {
            // We create an in-memory host store using the configuration passed in hostMonitorConfig.
            log.info("Creating in-memory host store");
            return createInMemoryStore(hostMonitorConfig);
        }
    }

    @VisibleForTesting
    public static HostControllerStore createInMemoryStore(HostMonitorConfig hostMonitorConfig) {
        log.info("Creating in-memory host store");
        Map<Host, Set<Integer>> hostContainerMap = new HashMap<>();
        hostContainerMap.put(new Host(hostMonitorConfig.getSssHost(), hostMonitorConfig.getSssPort()),
                IntStream.range(0, hostMonitorConfig.getContainerCount()).boxed().collect(Collectors.toSet()));
        return new InMemoryHostStore(hostContainerMap);
    }
}
