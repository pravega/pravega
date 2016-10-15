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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Choose a random H
 */
@Slf4j
public class RandomContainerBalancer implements ContainerBalancer<Integer, EndPoint> {

    private final CuratorFramework zkClient;
    private final String path = ZKPaths.makePath("cluster", "segmentContainerHostMapping");
    private final NodeCache segContainerHostMapping;

    public RandomContainerBalancer(CuratorFramework client) {
        zkClient = client;
        segContainerHostMapping = new NodeCache(zkClient, path);

        try {
            segContainerHostMapping.start();
        } catch (Exception e) {
            log.error("Error while fetching Segment container to Host mapping from container", e);
            throw new RuntimeException(e);
        }

    }

    @Override
    public CompletableFuture<Map<Integer, EndPoint>> rebalance(List<EndPoint> hostsAdded, List<EndPoint> hostsRemoved) {
        return null; //TODO: To be implemented
    }

    /*
       Fetch OwnedSegmentContainers from ZK
     */
    private CompletableFuture<List<Integer>> getOwnedSegmentContainers(EndPoint host) {
        //TODO: Refactor CompletableFuture
        Map<Integer, EndPoint> map = (Map<Integer, EndPoint>) SerializationUtils.deserialize(segContainerHostMapping.getCurrentData().getData());
        List<Integer> result = map.entrySet().stream()
                .filter(entry -> entry.getValue().equals(host))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        return CompletableFuture.completedFuture(result);
    }

    //Fetch the segment container to host mapping from zk.
    @Override
    public Map<Integer, EndPoint> getSegmentContainerHostMapping() {
        return (Map<Integer, EndPoint>) SerializationUtils.deserialize(segContainerHostMapping.getCurrentData().getData());
    }

    //Persist entries to ZK
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
