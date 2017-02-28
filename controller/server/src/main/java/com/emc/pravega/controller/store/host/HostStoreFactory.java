/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.host;

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.controller.util.ZKUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class HostStoreFactory {
    public enum StoreType {
        InMemory,
        Zookeeper
    }

    public static HostControllerStore createStore(StoreType type) {
        switch (type) {
        case InMemory:
            log.info("Creating in-memory host store");
            Map<Host, Set<Integer>> hostContainerMap = new HashMap<>();
            hostContainerMap.put(new Host(Config.SERVICE_HOST, Config.SERVICE_PORT),
                    IntStream.range(0, Config.HOST_STORE_CONTAINER_COUNT).boxed().collect(Collectors.toSet()));
            return new InMemoryHostStore(hostContainerMap);
            
        case Zookeeper:
            log.info("Creating Zookeeper based host store");
            return new ZKHostStore(ZKUtils.getCuratorClient());
            
        default:
            throw new NotImplementedException();
        }
    }
}
