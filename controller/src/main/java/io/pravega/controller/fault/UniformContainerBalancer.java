/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.fault;

import io.pravega.common.cluster.Host;
import io.pravega.controller.util.Config;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * This implements the ContainerBalancer by uniformly distributing Segment containers across all hosts in the cluster.
 * This is based only on the number of containers hosted on a given host and does not consider any other metric.
 *
 * The algorithm works as follows:
 * - Sort the hosts based on the number of containers they own.
 * - Repeat the following until the hosts are balanced.
 *     -- Remove a container from the host containing max number of containers.
 *     -- Add this to the host containing min number of containers.
 * - The balanced condition happens when the difference between min and max containers is at most 1.
 */
@Slf4j
public class UniformContainerBalancer implements ContainerBalancer {

    @Override
    public Map<Host, Set<Integer>> rebalance(Map<Host, Set<Integer>> prevSegContainerMap,
            Set<Host> currentHosts) {
        Preconditions.checkNotNull(prevSegContainerMap, "prevSegContainerMap");
        Preconditions.checkNotNull(currentHosts, "currentHosts");

        if (currentHosts.isEmpty()) {
            log.info("No hosts found during rebalancing, creating empty map");
            return new HashMap<>();
        }

        if (prevSegContainerMap.keySet().equals(currentHosts)) {
            //Assuming the input map is always balanced, since this balancer only depends on the host list.
            log.debug("No change in host list, using existing map");
            return new HashMap<>(prevSegContainerMap);
        }

        if (prevSegContainerMap.size() == 0 || currentHosts.size() == 1) {
            log.info("Creating new balanced map");
            return initializeMap(currentHosts);
        }

        //Detect hosts not present in the existing Map.
        Set<Host> hostsAdded = currentHosts.stream().
            filter(h -> !prevSegContainerMap.containsKey(h)).collect(Collectors.toSet());

        if (!hostsAdded.isEmpty()) {
            log.debug("New hosts added: {}", hostsAdded);
        }

        //Detect all containers whose host has been removed. These need to be assigned to new hosts.
        Set<Integer> orphanedContainers = prevSegContainerMap.entrySet().stream().
            filter(h -> !currentHosts.contains(h.getKey())).flatMap(p -> p.getValue().stream())
            .collect(Collectors.toSet());

        //Using a TreeSet sorted by number of containers to optimize add/remove operations.
        TreeSet<Map.Entry<Host, Set<Integer>>> mapElements =
            new TreeSet<>((o1, o2) -> {
                if (o1.getValue().size() < o2.getValue().size()) {
                    return -1;
                } else if (o1.getValue().size() > o2.getValue().size()) {
                    return 1;
                } else {
                    // Position elements with equal container length using object hashCode to provide strict weak
                    // ordering. We don't need any specific ordering using information from the host.
                    return o1.getKey().hashCode() - o2.getKey().hashCode();
                }
            });

        //Add the new hosts to the TreeSet.
        prevSegContainerMap.entrySet().stream().filter(h -> currentHosts.contains(h.getKey())).
            forEach(mapElements::add);
        hostsAdded.forEach(h -> mapElements.add(new AbstractMap.SimpleEntry<>(h, new HashSet<>())));

        //Add the orphaned containers into the TreeSet, while balancing it.
        for (Integer container : orphanedContainers) {
            Map.Entry<Host, Set<Integer>> first = mapElements.pollFirst();
            first.getValue().add(container);
            mapElements.add(first);
        }

        //Perform the rebalance, remove an element from host with max containers and add to host with min containers.
        //The loop is guaranteed to complete since we stop as soon as the diff between max and min is 1.
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

        //Create a Map from the TreeSet and return.
        Map<Host, Set<Integer>> newMap = new HashMap<>();
        mapElements.forEach(m -> newMap.put(m.getKey(), m.getValue()));

        log.info("Completed segment container rebalancing using new hosts set");
        return newMap;
    }

    private Map<Host, Set<Integer>> initializeMap(Set<Host> currentHosts) {
        final int containerCount = Config.HOST_STORE_CONTAINER_COUNT;

        //Assigned containers from 0 to HOST_STORE_CONTAINER_COUNT - 1 uniformly to all the hosts.
        PrimitiveIterator.OfInt intIter = IntStream.range(0, currentHosts.size()).iterator();
        Map<Host, Set<Integer>> segContainerMap = currentHosts.stream().collect(Collectors.toMap(
            Function.identity(),
            host ->
                Stream.of(intIter.next()).flatMap(i ->
                    IntStream.rangeClosed(0, containerCount / currentHosts.size()).boxed().
                        map(j -> j * currentHosts.size() + i)).filter(k -> k < containerCount).
                    collect(Collectors.toSet())
            )
        );

        return segContainerMap;
    }
}
