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

package com.emc.pravega.cluster.zkutils.dummy;

import com.emc.pravega.cluster.zkutils.abstraction.ConfigChangeListener;
import com.emc.pravega.cluster.zkutils.common.CommonConfigSyncManager;
import com.emc.pravega.cluster.zkutils.common.Endpoint;
import com.google.common.base.Preconditions;

import java.util.concurrent.ConcurrentHashMap;

public class DummyZK extends CommonConfigSyncManager {

    private final ConcurrentHashMap<String, byte[]> valueMap;

    public DummyZK(ConfigChangeListener listener) {
        super(listener);

        valueMap = new ConcurrentHashMap<>();
    }


    @Override
    public void createEntry(String path, byte[] value) {
        Preconditions.checkNotNull(path);
        Preconditions.checkNotNull(value);
        valueMap.put(path, value);
        if (path.startsWith(NODE_INFO_ROOT)) {
            listener.nodeAddedNotification(Endpoint.constructFrom(path));
        } else if (path.startsWith(CONTROLLER_INFO_ROOT)) {
            listener.controllerAddedNotification(Endpoint.constructFrom(path));
        }
    }

    @Override
    public void deleteEntry(String path) {
        Preconditions.checkNotNull(path);

        valueMap.remove(path);
        if (path.startsWith(NODE_INFO_ROOT)) {
            listener.nodeRemovedNotification(Endpoint.constructFrom(path));
        } else if (path.startsWith(CONTROLLER_INFO_ROOT)) {
            listener.controllerRemovedNotification(Endpoint.constructFrom(path));
        }
    }

    @Override
    public void refreshCluster() {

    }
}


