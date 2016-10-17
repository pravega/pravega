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
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This implements the ContainerBalancer by randomly assigning Segment container owners in the event hosts being removed.
 */
public class RandomContainerBalancer extends SegContainerHostMappingZK implements ContainerBalancer<Integer, EndPoint> {
    public RandomContainerBalancer(CuratorFramework client) {
        super(client);
    }

    @Override
    public Map<Integer, EndPoint> rebalance(List<EndPoint> hostsAdded, List<EndPoint> hostsRemoved) {
        //get the current list of container to Host mapping
        Map<Integer, EndPoint> segContainerMap = (Map<Integer, EndPoint>) SerializationUtils
                .deserialize(segContainerHostMapping.getCurrentData().getData());

        List<Integer> segContToBeUpdated = segContainerMap.entrySet().stream()
                .filter(ep -> hostsRemoved.contains(ep.getValue()))
                .map(ep -> ep.getKey())
                .collect(Collectors.toList());

        List<EndPoint> validEndPoints = new ArrayList<>(segContainerMap.values().stream()
                .filter(ep -> !hostsRemoved.contains(ep))
                .collect(Collectors.toSet()));
        validEndPoints.addAll(hostsAdded);

        //choose a random Endpoint
        segContToBeUpdated.stream().map(index -> segContainerMap.put(index, validEndPoints.get(index % validEndPoints.size())));
        return segContainerMap;
    }
}
