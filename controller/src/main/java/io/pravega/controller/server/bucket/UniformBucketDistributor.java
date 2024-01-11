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

package io.pravega.controller.server.bucket;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.pravega.common.hash.HashHelper;
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
import lombok.extern.slf4j.Slf4j;

/**
 * This implements the BucketDistributor by uniformly distributing buckets across all controllers in the cluster.
 * This is based only on the number of buckets running on a given controller and does not consider any other metric.
 *
 * The algorithm works as follows:
 * - Sort the controllers based on the number of buckets they own.
 * - Repeat the following until the controllers are balanced.
 *     -- Remove a bucket from the controller containing max number of buckets.
 *     -- Add this to the controller containing min number of buckets.
 * - The balanced condition happens when the difference between min and max buckets is at most 1.
 */
@Slf4j
public class UniformBucketDistributor implements BucketDistributor {
    @Override
    public Map<String, Set<Integer>> distribute(Map<String, Set<Integer>> previousBucketControllerMapping,
                                                 Set<String> currentControllers, int bucketCount) {
        Preconditions.checkNotNull(previousBucketControllerMapping, "previousBucketControllerMapping");
        Preconditions.checkNotNull(currentControllers, "currentControllers");
        if (previousBucketControllerMapping.keySet().equals(currentControllers)) {
            //Assuming the input map is always uniform distributed , since this distributor only depends on the controllers list.
            log.debug("No change in controller list, using existing map {}", previousBucketControllerMapping);
            return new HashMap<>(previousBucketControllerMapping);
        }

        if (previousBucketControllerMapping.isEmpty() || currentControllers.size() == 1) {
            log.info("Creating new balanced map for controllers {}", currentControllers);
            return initializeMap(currentControllers, bucketCount);
        }

        //Detect controllers not present in the existing Map.
        Set<String> controllersAdded = currentControllers.stream().
                                           filter(h -> !previousBucketControllerMapping.containsKey(h)).collect(Collectors.toSet());
        if (!controllersAdded.isEmpty()) {
            log.debug("New controllers added: {}", controllersAdded);
        }
        //Detect all buckets whose controllers has been removed. These need to be assigned to new controllers.
        Set<Integer> orphanedBuckets = previousBucketControllerMapping.entrySet().stream().
                                                             filter(h -> !currentControllers.contains(h.getKey()))
                                                                   .flatMap(p -> p.getValue().stream())
                                                                   .collect(Collectors.toSet());

        //Using a TreeSet sorted by number of buckets to optimize add/remove operations.
        TreeSet<Map.Entry<String, Set<Integer>>> mapElements =
                new TreeSet<>((o1, o2) -> {
                    if (o1.getValue().size() < o2.getValue().size()) {
                        return -1;
                    } else if (o1.getValue().size() > o2.getValue().size()) {
                        return 1;
                    } else {
                        // Position elements with equal bucket length using object hashCode to provide strict weak
                        // ordering. We don't need any specific ordering using information from the controller.
                        HashHelper hashHelper = HashHelper.seededWith("ControllerHostId");
                        return (int) (hashHelper.hash(o1.getKey()) - hashHelper.hash(o2.getKey()));
                    }
                });

        //Add the new controllers to the TreeSet.
        previousBucketControllerMapping.entrySet().stream().filter(h -> currentControllers.contains(h.getKey())).
                           forEach(mapElements::add);
        controllersAdded.forEach(h -> mapElements.add(new AbstractMap.SimpleEntry<>(h, new HashSet<>())));

        //Add the orphaned buckets into the TreeSet, while balancing it.
        for (Integer bucket : orphanedBuckets) {
            Map.Entry<String, Set<Integer>> first = mapElements.pollFirst();
            ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
            first.setValue(builder.addAll(first.getValue()).add(bucket).build());
            mapElements.add(first);
        }

        //Perform the distribution, remove an element from controller with max buckets and add to controller with min buckets.
        //The loop is guaranteed to complete since we stop as soon as the diff between max and min is 1.
        while ((mapElements.last().getValue().size()
                - mapElements.first().getValue().size()) > 1) {

            Map.Entry<String, Set<Integer>> first = mapElements.pollFirst();
            Map.Entry<String, Set<Integer>> last = mapElements.pollLast();

            Integer removeCont = last.getValue().iterator().next();
            last.setValue(ImmutableSet.copyOf(Sets.difference( last.getValue(), ImmutableSet.of(removeCont))));
            ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
            first.setValue(builder.addAll(first.getValue()).add(removeCont).build());

            mapElements.add(first);
            mapElements.add(last);
        }

        //Create a Map from the TreeSet and return.
        Map<String, Set<Integer>> newMap = new HashMap<>();
        mapElements.forEach(m -> newMap.put(m.getKey(), m.getValue()));

        log.info("Completed bucket distribution using new controllers set : {}. New mapping is {}.",
                currentControllers, newMap);

        return newMap;
    }

    private Map<String, Set<Integer>> initializeMap(Set<String> currentControllers, int bucketCount) {
        //Assigned buckets from 0 to bucket count - 1 uniformly to all the controllers.
        PrimitiveIterator.OfInt intIter = IntStream.range(0, currentControllers.size()).iterator();
        Map<String, Set<Integer>> bucketControllerMap = currentControllers.stream().collect(Collectors.toMap(
                        Function.identity(),
                        controller ->
                                Stream.of(intIter.next()).flatMap(i ->
                                              IntStream.rangeClosed(0, bucketCount / currentControllers.size()).boxed().
                                                       map(j -> j * currentControllers.size() + i)).filter(k -> k < bucketCount).
                                      collect(Collectors.toSet())
                )
        );
        return bucketControllerMap;
    }
}
