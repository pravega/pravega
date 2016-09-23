/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.store.host;

import com.emc.pravega.controller.store.stream.StoreConfiguration;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.emc.pravega.controller.util.Config.HOST_STORE_CONTAINER_COUNT;

public class InMemoryHostControllerStoreConfig implements StoreConfiguration {
    private final int numOfContainers = HOST_STORE_CONTAINER_COUNT;

    private final Map<Host, Set<Integer>> hostContainerMap;

    public InMemoryHostControllerStoreConfig(Map<Host, Set<Integer>> hostContainerMap) {
        this.hostContainerMap = hostContainerMap;
    }

    public Map<Host, Set<Integer>> getHostContainerMap() {
        return Collections.unmodifiableMap(hostContainerMap);
    }

    public int getNumOfContainers() {
        return numOfContainers;
    }
}
