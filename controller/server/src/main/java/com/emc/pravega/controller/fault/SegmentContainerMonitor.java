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

import com.emc.pravega.controller.util.ZKUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ZKPaths;

/**
 * Class used to monitor the Data nodes for failures and ensure the segment containers owned by them is assigned
 * to the other Data nodes.
 */
@Slf4j
public class SegmentContainerMonitor implements AutoCloseable {

    private final SegmentMonitorLeader segmentMonitorLeader;
    private final LeaderSelector leaderSelector;

    public SegmentContainerMonitor(CuratorFramework client, String clusterName) {

        final String leaderZKPath = ZKPaths.makePath("cluster", clusterName, "data", "faulthandlerleader");
        ZKUtils.createPathIfNotExists(client, leaderZKPath);

        segmentMonitorLeader = new SegmentMonitorLeader(client, clusterName);
        leaderSelector = new LeaderSelector(client, leaderZKPath, segmentMonitorLeader);

        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                if (!newState.isConnected()) {
                    leaderSelector.interruptLeadership();
                }
            }
        });

        leaderSelector.autoRequeue();
        leaderSelector.start();
    }

    @Override
    public void close() {
        leaderSelector.interruptLeadership();
        leaderSelector.close();
    }
}
