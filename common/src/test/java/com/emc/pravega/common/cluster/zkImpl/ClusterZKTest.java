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

import com.emc.pravega.common.cluster.EndPoint;
import com.emc.pravega.common.cluster.NodeType;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;


public class ClusterZKTest {

    private static final String ZK_URL = "localhost:2181";
    private final static String HOST_1 = "host1";
    private final static String HOST_2 = "host2";
    private final static int PORT = 1234;
    private final static String CLUSTER_NAME = "cluster-1";

    @Test
    public void registerNode() throws Exception {
        //TestClusterListener for testing purposes
        TestClusterListener clusterListener = new TestClusterListener(ZK_URL, CLUSTER_NAME, NodeType.DATA);
        clusterListener.start();

        //Create Add a node to the cluster.
        ClusterZKImpl clusterZKInstance1 = new ClusterZKImpl(ZK_URL, CLUSTER_NAME, NodeType.DATA);
        clusterZKInstance1.registerNode(new EndPoint(HOST_1, PORT));
        assertEquals(HOST_1, clusterListener.nodeAddedQueue.poll(5, TimeUnit.SECONDS));

        //Create a seperate instance of Cluster and add node to same Cluster
        ClusterZKImpl clusterZKInstance2 = new ClusterZKImpl(ZK_URL, CLUSTER_NAME, NodeType.DATA);
        clusterZKInstance1.registerNode(new EndPoint(HOST_2, PORT));
        assertEquals(HOST_2, clusterListener.nodeAddedQueue.poll(5, TimeUnit.SECONDS));
    }

    @Test
    public void deregisterNode() throws Exception {
        TestClusterListener clusterListener = new TestClusterListener(ZK_URL, CLUSTER_NAME, NodeType.DATA);
        clusterListener.start();

        //Create Add a node to the cluster.
        ClusterZKImpl clusterZKInstance1 = new ClusterZKImpl(ZK_URL, CLUSTER_NAME, NodeType.DATA);
        clusterZKInstance1.registerNode(new EndPoint(HOST_1, PORT));
        assertEquals(HOST_1, clusterListener.nodeAddedQueue.poll(5, TimeUnit.SECONDS));

        clusterZKInstance1.deregisterNode(new EndPoint(HOST_1, PORT));
        assertEquals(HOST_1, clusterListener.nodeRemovedQueue.poll(5, TimeUnit.SECONDS));

    }
}