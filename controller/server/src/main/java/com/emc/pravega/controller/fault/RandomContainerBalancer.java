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
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        Map<Integer, Host> segContainerMap = (Map<Integer, Host>) SerializationUtils
                .deserialize(segContainerHostMapping.getCurrentData().getData());
        /*
            TODO:
            1. IF (map returned is empty OR map size is less than configuration)
                INITIALLZE The map.
            2. Assert on invalid input : HostsPresent and hostsRemoved.

         */
        List<Integer> segContToBeUpdated = segContainerMap.entrySet().stream()
                .filter(ep -> hostsRemoved.contains(ep.getValue()))
                .map(ep -> ep.getKey())
                .collect(Collectors.toList());

        List<Host> validHosts = new ArrayList<>(segContainerMap.values().stream()
                .filter(ep -> !hostsRemoved.contains(ep))
                .collect(Collectors.toSet()));
        validHosts.addAll(hostsPresent);

        //choose a random Host
        segContToBeUpdated.stream().map(index -> segContainerMap.put(index, validHosts.get(index % validHosts.size())));
        return segContainerMap;
    }
}
