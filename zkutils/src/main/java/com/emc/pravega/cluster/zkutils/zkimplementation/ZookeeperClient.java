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
package com.emc.pravega.cluster.zkutils.zkimplementation;

import com.emc.pravega.cluster.zkutils.abstraction.ConfigSyncManager;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class ZookeeperClient
  extends ZooKeeper implements Watcher, ConfigSyncManager {

    public ZookeeperClient(String connectString, int sessionTimeout) throws IOException {
        super(connectString, sessionTimeout,null);
    }

    String pravegaNodesPath = "/pravega/nodes";
    String pravegaControllersPath = "/pravega/controllers";

    /**
     * Sample configuration/synchronization methods. Will add more as implementation progresses
     *
     * @param path
     * @param value
     */
    @Override
    public String createEntry(String path, byte[] value) {
        return null;
    }

    @Override
    public void deleteEntry(String path) {

    }

    @Override
    public void refreshCluster() {

    }

    @Override
    public void registerPravegaNode(String host, int port, String jsonMetadata) {

    }

    @Override
    public void registerPravegaController(String host, int port, String jsonMetadata) {

    }

    @Override
    public void process(WatchedEvent event) {

    }
}

