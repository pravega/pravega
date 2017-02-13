/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
            return new ZKHostStore(ZKUtils.getCuratorClient(), Config.CLUSTER_NAME);
            
        default:
            throw new NotImplementedException();
        }
    }
}
