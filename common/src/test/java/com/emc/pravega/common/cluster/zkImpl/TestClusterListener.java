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
package com.emc.pravega.common.cluster.zkImpl;

import com.emc.pravega.common.cluster.NodeType;

import java.util.concurrent.LinkedBlockingQueue;

public class TestClusterListener extends ClusterListenerZKImpl {
    public LinkedBlockingQueue<String> nodeAddedQueue = new LinkedBlockingQueue();
    public LinkedBlockingQueue<String> nodeRemovedQueue = new LinkedBlockingQueue();

    public TestClusterListener(String connectionString, String clusterName, NodeType type) {
        super(connectionString, clusterName, type);
    }

    /**
     * Method invoked when node has been added
     *
     * @param hostName
     */
    @Override
    public void nodeAdded(String hostName) {
        nodeAddedQueue.offer(hostName);
    }

    /**
     * Method invoked when node has been removed
     *
     * @param hostName
     */
    @Override
    public void nodeRemoved(String hostName) {
        nodeRemovedQueue.offer(hostName);
    }
}
