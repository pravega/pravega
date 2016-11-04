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

import com.emc.pravega.common.cluster.Host;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.utils.ZKPaths;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.emc.pravega.controller.util.ZKUtils.createPathIfNotExists;

/**
 * ZK based implementation for SegmentContainer to HostMapping.
 */
@Slf4j
public class SegContainerHostMappingZK implements SegContainerHostMapping<Host, Set<Integer>> {

    private final NodeCache segContainerHostMapping;

    private final String zkPath;
    private final CuratorFramework zkClient;

    public SegContainerHostMappingZK(CuratorFramework client, String clusterName) {
        zkClient = client;
        try {
            zkPath = ZKPaths.makePath("cluster", clusterName, "segmentContainerHostMapping");
            createPathIfNotExists(zkClient, zkPath, SerializationUtils.serialize(new HashMap<Host, Set<Integer>>()));
            segContainerHostMapping = new NodeCache(zkClient, zkPath);
            segContainerHostMapping.start(true);
        } catch (Exception e) {
            log.error("Error while fetching Segment container to Host mapping from container", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Read mapping entries from ZK.
     *
     * @return
     */
    @Override
    public Map<Host, Set<Integer>> getSegmentContainerHostMapping() {
        Optional<ChildData> containerToHostMapSer = Optional.ofNullable(segContainerHostMapping.getCurrentData());
        Map<Host, Set<Integer>> mapping;
        if (containerToHostMapSer.isPresent()) {
            mapping = (HashMap<Host, Set<Integer>>) SerializationUtils.deserialize(containerToHostMapSer.get().getData());
        } else {
            mapping = new HashMap<>(); //create empty map
        }
        return mapping;
    }

    /**
     * Persist mapping entries to ZK.
     *
     * @param map - Map of SegmentContainer to Host.
     */
    @Override
    public void updateSegmentContainerHostMapping(Map<Host, Set<Integer>> map) {
        try {
            zkClient.setData().forPath(zkPath, SerializationUtils.serialize((HashMap) map));
        } catch (Exception e) {
            throw new RuntimeException("Error while persisting segment container to host mapping", e);
        }
    }
}
