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
import com.emc.pravega.controller.util.Config;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UniformContainerBalancerTest {

    @Test
    public void testRebalancer() {

        UniformContainerBalancer balancer = new UniformContainerBalancer();

        //Validate empty host.
        HashSet<Host> hosts = new HashSet<>();
        Optional<Map<Host, Set<Integer>>> rebalance = balancer.rebalance(new HashMap<>(), hosts);
        assertEquals(0, rebalance.get().size());

        //Validate initialization.
        hosts.add(new Host("host1", 123));
        rebalance = balancer.rebalance(new HashMap<>(), hosts);
        assertEquals(1, rebalance.get().size());
        validateContainerCount(rebalance.get(), hosts);

        //New host added.
        hosts.add(new Host("host2", 123));
        rebalance = balancer.rebalance(rebalance.get(), hosts);
        assertEquals(2, rebalance.get().size());
        validateContainerCount(rebalance.get(), hosts);

        //Add multiple hosts.
        hosts.add(new Host("host3", 123));
        hosts.add(new Host("host4", 123));
        rebalance = balancer.rebalance(rebalance.get(), hosts);
        assertEquals(4, rebalance.get().size());
        validateContainerCount(rebalance.get(), hosts);

        //Remove host.
        hosts.remove(new Host("host2", 123));
        rebalance = balancer.rebalance(rebalance.get(), hosts);
        assertEquals(3, rebalance.get().size());
        validateContainerCount(rebalance.get(), hosts);

        //Add and remove multiple hosts.
        hosts.add(new Host("host2", 123));
        hosts.add(new Host("host5", 123));
        hosts.add(new Host("host6", 123));
        hosts.add(new Host("host7", 123));
        hosts.add(new Host("host8", 123));
        hosts.add(new Host("host9", 123));
        hosts.remove(new Host("host1", 123));
        hosts.remove(new Host("host3", 123));
        hosts.remove(new Host("host4", 123));
        rebalance = balancer.rebalance(rebalance.get(), hosts);
        assertEquals(6, rebalance.get().size());
        validateContainerCount(rebalance.get(), hosts);

        //Remove multiple hosts.
        hosts.remove(new Host("host2", 123));
        hosts.remove(new Host("host5", 123));
        hosts.remove(new Host("host6", 123));
        rebalance = balancer.rebalance(rebalance.get(), hosts);
        assertEquals(3, rebalance.get().size());
        validateContainerCount(rebalance.get(), hosts);

        //No changes.
        rebalance = balancer.rebalance(rebalance.get(), hosts);
        assertEquals(3, rebalance.get().size());
        validateContainerCount(rebalance.get(), hosts);
    }

    private void validateContainerCount(Map<Host, Set<Integer>> containerMap, Set<Host> hosts) {

        long contCount = containerMap.values().stream().map(m -> m.size()).reduce((a, b) -> a + b).get();
        assertTrue(contCount == Config.HOST_STORE_CONTAINER_COUNT);

        Set<Integer> containersInMap = containerMap.values().stream().
                flatMap(m -> m.stream()).collect(Collectors.toSet());
        assertEquals(containersInMap,
                IntStream.range(0, Config.HOST_STORE_CONTAINER_COUNT).boxed().collect(Collectors.toSet()));

        long maxContCount = containerMap.entrySet().stream().map(
                m -> m.getValue().size()).max((a, b) -> a.compareTo(b)).get();
        long minContCount = containerMap.entrySet().stream().map(
                m -> m.getValue().size()).min((a, b) -> a.compareTo(b)).get();
        assertTrue(maxContCount - minContCount <= 1);

        //Verify the hosts in the map matches the expected hosts.
        assertEquals(containerMap.keySet(), hosts);
    }

}
