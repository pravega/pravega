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
package com.emc.pravega.controller.fault;

import com.emc.pravega.common.cluster.zkImpl.ClusterListenerZKImpl;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentContainerRecovery extends ClusterListenerZKImpl {

    public SegmentContainerRecovery(String connectionString, String clusterName) {
        super(connectionString, clusterName);
    }

    /**
     * Method invoked when node has been added
     *
     * @param hostName
     */
    @Override
    public void nodeAdded(String hostName) {
        log.info("Node {} added to cluster", hostName, getClusterName());
        //Steps to be performed on Segment containers.
        //1. Rebalance the segment containers across the nodes.
    }

    /**
     * Method invoked when node has been removed
     *
     * @param hostName
     */
    @Override
    public void nodeRemoved(String hostName) {
        log.info("Node {} removed from cluster", hostName, getClusterName());
        //Steps to be performed for Segment container recovery
        //1. Get the list of segment containers owned by this node.
        //2. For-Each Segment Container
        //  2.1 Find the ensemble information from bookkeeper
        //  2.2. Choose the best host to reassign the containers.(Elaborate the logic)
        //  2.3 Assign the container to the right host.
    }
}
