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

package com.emc.pravega.cluster.zkutils.abstraction;

import com.emc.pravega.cluster.zkutils.dummy.DummyZK;
import com.emc.pravega.cluster.zkutils.vnest.Vnest;
import com.emc.pravega.cluster.zkutils.zkimplementation.ZookeeperClient;


public final class ConfigSyncManagerFactory {
    public ConfigSyncManager createManager(ConfigSyncManagerType type, String connectionString, int timeoutms,
                                           ConfigChangeListener listener) throws Exception {
        switch (type) {
            case DUMMY:
                return new DummyZK(listener);
            case ZK:
                    return new ZookeeperClient(connectionString, timeoutms, listener);
            case VNEST:
                return new Vnest(connectionString, timeoutms, listener);
            default:
                throw new UnsupportedOperationException("ConfigSyncManager with type " + type + "is not supported");

        }
    }
}
