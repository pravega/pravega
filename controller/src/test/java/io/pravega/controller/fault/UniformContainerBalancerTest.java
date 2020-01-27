/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.fault;

import io.pravega.common.cluster.Host;
import io.pravega.controller.util.Config;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UniformContainerBalancerTest {

    //Ensure test completes within 5 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(5, TimeUnit.SECONDS);

    @Test
    public void testRebalancer() {
        UniformContainerBalancer balancer = new UniformContainerBalancer();

        //Validate empty host.
        HashSet<Host> hosts = new HashSet<>();
        Map<Host, Set<Integer>> rebalance = balancer.rebalance(new HashMap<>(), hosts);
        assertEquals(0, rebalance.size());

        //Validate initialization.
        hosts.add(new Host("host1", 123, null));
        rebalance = balancer.rebalance(new HashMap<>(), hosts);
        assertEquals(1, rebalance.size());
        validateContainerCount(rebalance, hosts);

        //New host added.
        hosts.add(new Host("host2", 123, null));
        rebalance = balancer.rebalance(rebalance, hosts);
        assertEquals(2, rebalance.size());
        validateContainerCount(rebalance, hosts);

        //Add multiple hosts.
        hosts.add(new Host("host3", 123, null));
        hosts.add(new Host("host4", 123, null));
        rebalance = balancer.rebalance(rebalance, hosts);
        assertEquals(4, rebalance.size());
        validateContainerCount(rebalance, hosts);

        //Remove host.
        hosts.remove(new Host("host2", 123, null));
        rebalance = balancer.rebalance(rebalance, hosts);
        assertEquals(3, rebalance.size());
        validateContainerCount(rebalance, hosts);

        //Add and remove multiple hosts.
        hosts.add(new Host("host2", 123, null));
        hosts.add(new Host("host5", 123, null));
        hosts.add(new Host("host6", 123, null));
        hosts.add(new Host("host7", 123, null));
        hosts.add(new Host("host8", 123, null));
        hosts.add(new Host("host9", 123, null));
        hosts.remove(new Host("host1", 123, null));
        hosts.remove(new Host("host3", 123, null));
        hosts.remove(new Host("host4", 123, null));
        rebalance = balancer.rebalance(rebalance, hosts);
        assertEquals(6, rebalance.size());
        validateContainerCount(rebalance, hosts);

        //Remove multiple hosts.
        hosts.remove(new Host("host2", 123, null));
        hosts.remove(new Host("host5", 123, null));
        hosts.remove(new Host("host6", 123, null));
        rebalance = balancer.rebalance(rebalance, hosts);
        assertEquals(3, rebalance.size());
        validateContainerCount(rebalance, hosts);

        //No changes.
        rebalance = balancer.rebalance(rebalance, hosts);
        assertEquals(3, rebalance.size());
        validateContainerCount(rebalance, hosts);
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
