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
package com.emc.pravega.controller.store;

import com.emc.pravega.controller.util.ZKUtils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.CuratorFramework;

/**
 * ZK client.
 */
public class ZKStoreClient implements StoreClient {

    private final CuratorFramework client;

    public ZKStoreClient() {
        this.client = ZKUtils.getCuratorClient();
    }

    @VisibleForTesting
    public ZKStoreClient(CuratorFramework client) {
        this.client = client;
    }

    @Override
    public CuratorFramework getClient() {
        return this.client;
    }

    @Override
    public StoreClientFactory.StoreType getType() {
        return StoreClientFactory.StoreType.Zookeeper;
    }
}
