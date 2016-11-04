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
import com.google.common.base.Preconditions;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This implements the ContainerBalancer by uniformly distributing Segment containers across all hosts in the cluster.
 * This is based only on the number of containers hosted on a given host and does not consider any other metric.
 *
 * The algorithm works as follows:
 *
 *
 */
public class UniformContainerBalancer implements ContainerBalancer<Host, Set<Integer>> {

    @Override
    public Optional<Map<Host, Set<Integer>>> rebalance(Map<Host, Set<Integer>> prevSegContainerMap,
            Set<Host> currentHosts) {

        Preconditions.checkNotNull(prevSegContainerMap, "prevSegContainerMap");
        Preconditions.checkNotNull(currentHosts, "currentHosts");

        if (currentHosts.isEmpty()) {
            return Optional.empty();
        }

        if (prevSegContainerMap.keySet().equals(currentHosts)) {
            return Optional.of(new HashMap<>(prevSegContainerMap));
        }

        if (prevSegContainerMap.size() == 0 || currentHosts.size() == 1) {
            return initializeMap(currentHosts);
        }

        Set<Host> hostsAdded = currentHosts.stream().
            filter(h -> !prevSegContainerMap.containsKey(h)).collect(Collectors.toSet());

        Set<Integer> orphanedContainers = prevSegContainerMap.entrySet().stream().
            filter(h -> !currentHosts.contains(h.getKey())).flatMap(p -> p.getValue().stream())
            .collect(Collectors.toSet());

        TreeSet<Map.Entry<Host, Set<Integer>>> mapElements =
            new TreeSet<>((o1, o2) -> o1.getValue().size() > o2.getValue().size() ? 1 : -1);

        prevSegContainerMap.entrySet().stream().filter(h -> currentHosts.contains(h.getKey())).
            forEach(mapElements::add);
        hostsAdded.forEach(h -> mapElements.add(new AbstractMap.SimpleEntry<>(h, new HashSet<>())));

        for (Integer container : orphanedContainers) {
            Map.Entry<Host, Set<Integer>> first = mapElements.pollFirst();
            first.getValue().add(container);
            mapElements.add(first);
        }

        while ((mapElements.last().getValue().size()
                - mapElements.first().getValue().size()) > 1) {

            Map.Entry<Host, Set<Integer>> first = mapElements.pollFirst();
            Map.Entry<Host, Set<Integer>> last = mapElements.pollLast();

            Integer removeCont = last.getValue().iterator().next();
            last.getValue().remove(removeCont);
            first.getValue().add(removeCont);

            mapElements.add(first);
            mapElements.add(last);
        }

        Map<Host, Set<Integer>> newMap = new HashMap<>();
        mapElements.forEach(m -> newMap.put(m.getKey(), m.getValue()));

        return Optional.of(newMap);
    }

    private Optional<Map<Host, Set<Integer>>> initializeMap(Set<Host> currentHosts) {

        final int containerCount = Config.HOST_STORE_CONTAINER_COUNT;

        PrimitiveIterator.OfInt intIter = IntStream.range(0, currentHosts.size()).iterator();
        Map<Host, Set<Integer>> segContainerMap = currentHosts.stream().collect(Collectors.toMap(
            java.util.function.Function.identity(),
            host ->
                java.util.stream.Stream.of(intIter.next()).flatMap(i ->
                    IntStream.rangeClosed(0, containerCount / currentHosts.size()).boxed().
                        map(j -> j * currentHosts.size() + i)).filter(k -> k < containerCount).
                    collect(Collectors.toSet())
            )
        );

        return Optional.of(segContainerMap);
    }
}
