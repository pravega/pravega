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

import com.emc.pravega.common.cluster.EndPoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

import java.util.HashMap;
import java.util.Map;

/**
 * ZK based implementation for SegmentContainer to HostMapping.
 */
@Slf4j
public class SegContainerHostMappingZK implements SegContainerHostMapping<Integer, EndPoint> {

    protected final NodeCache segContainerHostMapping;
    private final CuratorFramework zkClient;
    private final String path = ZKPaths.makePath("cluster", "segmentContainerHostMapping");

    public SegContainerHostMappingZK(CuratorFramework client) {
        zkClient = client;
        segContainerHostMapping = new NodeCache(zkClient, path);

        try {
            segContainerHostMapping.start();
        } catch (Exception e) {
            log.error("Error while fetching Segment container to Host mapping from container", e);
            throw new RuntimeException(e);
        }

    }

    /**
     * Read mapping entries from ZK.
     * @return
     */
    @Override
    public Map<Integer, EndPoint> getSegmentContainerHostMapping() {
        return (Map<Integer, EndPoint>) SerializationUtils.deserialize(segContainerHostMapping.getCurrentData().getData());
    }

    /**
     * Persist mapping entries to ZK.
     * @param map - Map of SegmentContainer to Host.
     */
    @Override
    public void persistSegmentContainerHostMapping(Map<Integer, EndPoint> map) {
        try {
            createPathIfExists(path);
            zkClient.create().forPath(path, SerializationUtils.serialize((HashMap) map));
        } catch (Exception e) {
            throw new RuntimeException("Error while persisting segment container to host mapping", e);
        }
    }

    private void createPathIfExists(String basePath) throws Exception {
        try {
            if (zkClient.checkExists().forPath(basePath) == null) {
                zkClient.create().creatingParentsIfNeeded().forPath(basePath);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Path exists {} , ignoring exception", basePath, e);
        }
    }
}
