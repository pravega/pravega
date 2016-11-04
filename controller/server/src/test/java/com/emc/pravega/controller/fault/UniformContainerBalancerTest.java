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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UniformContainerBalancerTest {

    @Test
    public void testInitialMap() {

        UniformContainerBalancer balancer = new UniformContainerBalancer();

        HashSet<Host> hosts = new HashSet<>();
        hosts.add(new Host("host1", 123));
        Optional<Map<Host, Set<Integer>>> rebalance = balancer.rebalance(new HashMap<>(), hosts);
        assertEquals(rebalance.get().size(), 1);
        validateContainerCount(rebalance.get());

        hosts.add(new Host("host2", 123));
        rebalance = balancer.rebalance(rebalance.get(), hosts);
        assertEquals(rebalance.get().size(), 2);
        validateContainerCount(rebalance.get());

        hosts.add(new Host("host3", 123));
        hosts.add(new Host("host4", 123));
        rebalance = balancer.rebalance(rebalance.get(), hosts);
        assertEquals(rebalance.get().size(), 4);
        validateContainerCount(rebalance.get());

        hosts.remove(new Host("host2", 123));
        rebalance = balancer.rebalance(rebalance.get(), hosts);
        assertEquals(rebalance.get().size(), 3);
        validateContainerCount(rebalance.get());

        hosts.add(new Host("host2", 123));
        hosts.add(new Host("host5", 123));
        hosts.add(new Host("host6", 123));
        hosts.add(new Host("host7", 123));
        hosts.add(new Host("host8", 123));
        hosts.add(new Host("host9", 123));
        rebalance = balancer.rebalance(rebalance.get(), hosts);
        assertEquals(rebalance.get().size(), 9);
        validateContainerCount(rebalance.get());

        hosts.remove(new Host("host1", 123));
        hosts.remove(new Host("host3", 123));
        hosts.remove(new Host("host5", 123));
        hosts.remove(new Host("host7", 123));
        rebalance = balancer.rebalance(rebalance.get(), hosts);
        assertEquals(rebalance.get().size(), 5);
        validateContainerCount(rebalance.get());

        rebalance = balancer.rebalance(rebalance.get(), hosts);
        assertEquals(rebalance.get().size(), 5);
        validateContainerCount(rebalance.get());
    }

    void validateContainerCount(Map<Host, Set<Integer>> containerMap) {

        long maxContEntry = containerMap.values().stream().flatMap(
                m -> m.stream()).max((a, b) -> a.compareTo(b)).get();
        long minContEntry = containerMap.values().stream().flatMap(
                m -> m.stream()).min((a, b) -> a.compareTo(b)).get();
        assertTrue(minContEntry == 0 && maxContEntry == Config.HOST_STORE_CONTAINER_COUNT - 1);

        long count1 = containerMap.values().stream().map(m -> m.size()).reduce((a, b) -> a + b).get();
        long count2 = containerMap.values().stream().flatMap(m -> m.stream()).distinct().count();
        assertTrue(count1 == count2 && count1 == Config.HOST_STORE_CONTAINER_COUNT);

        long maxContCount = containerMap.entrySet().stream().map(
                m -> m.getValue().size()).max((a, b) -> a.compareTo(b)).get();
        long minContCount = containerMap.entrySet().stream().map(
                m -> m.getValue().size()).min((a, b) -> a.compareTo(b)).get();
        assertTrue(maxContCount - minContCount <= 1);
    }

}
