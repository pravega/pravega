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
import org.apache.curator.framework.CuratorFramework;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.emc.pravega.controller.util.Config.HOST_STORE_CONTAINER_COUNT;

/**
 * This implements the ContainerBalancer by randomly assigning Segment container owners in the event hosts being removed.
 */
public class RandomContainerBalancer extends SegContainerHostMappingZK implements ContainerBalancer<Integer, Host> {
    public RandomContainerBalancer(CuratorFramework client) {
        super(client);
    }

    @Override
    public Map<Integer, Host> rebalance(List<Host> hostsPresent, List<Host> hostsRemoved) {
        //get the current list of container to Host mapping
        Map<Integer, Host> segContainerMap = getSegmentContainerHostMapping();

        checkAndInitialize(segContainerMap);

        List<Integer> segContToBeUpdated = segContainerMap.entrySet().stream()
                .filter(ep -> ep.getValue() == null || hostsRemoved.contains(ep.getValue()))
                .map(ep -> ep.getKey())
                .collect(Collectors.toList());

        //choose a random Host
        segContToBeUpdated.stream().forEach(index -> segContainerMap.put(index, hostsPresent.get(index % hostsPresent.size())));
        return segContainerMap;
    }

    private void checkAndInitialize(Map<Integer, Host> segContainerMap) {
        if (segContainerMap.keySet().size() < HOST_STORE_CONTAINER_COUNT) {
            IntStream.rangeClosed(0, HOST_STORE_CONTAINER_COUNT).forEach(i -> {
                if (!segContainerMap.containsKey(i)) {
                    segContainerMap.put(i, null);
                }
            });
        }
    }
}
