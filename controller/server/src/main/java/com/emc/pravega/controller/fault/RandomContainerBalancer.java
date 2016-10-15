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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Choose a random host Strategy
 * TODO:abstract zk out of picture.
 */
public class RandomContainerBalancer implements ContainerBalancer<Integer, EndPoint> {


    @Override
    public CompletableFuture<Map<Integer, EndPoint>> hostRemoved(EndPoint hostRemoved, List<EndPoint> availableHosts) {
        return getOwnedSegmentContainers(hostRemoved).thenApply(
                list -> list.stream().collect(Collectors.toMap(i -> i, i -> availableHosts.get(i))));
    }

    @Override
    public CompletableFuture<Map<Integer, EndPoint>> hostAdded(EndPoint host, List<EndPoint> availableHosts) {
        return null; //TODO
    }

    @Override
    public CompletableFuture<Map<Integer, EndPoint>> recompute(Map<Integer, EndPoint> segmentToErrorHost) {
        //TODO: is it required?
        return null;
    }

    /*
        Fetch OwnedSegmentContainers from ZK
     */
    private CompletableFuture<List<Integer>> getOwnedSegmentContainers(EndPoint host) {
        //TODO: Read for zk, Hardcoded as of now
        return CompletableFuture.completedFuture(Arrays.asList(1, 2, 3));
    }

    //Read entries from ZK
    @Override
    public CompletableFuture<Map<Integer, EndPoint>> getSegmentContainerHostMapping() {
        //TODO: Read from zk
        Map<Integer, EndPoint> map = new HashMap<>();
        map.put(1, new EndPoint("ServerA", 1234));
        map.put(1, new EndPoint("ServerB", 1234));
        map.put(1, new EndPoint("ServerC", 1234));
        return CompletableFuture.completedFuture(map);
    }

    //Persist entries to ZK
    @Override
    public CompletableFuture<Void> persistSegmentContainerHostMapping(Map<Integer, EndPoint> map) {
        return CompletableFuture.completedFuture(null);
    }
}
