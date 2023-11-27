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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * UniformBucketDistributorTest verifies uniform distribution of buckets.
 */
public class UniformBucketDistributorTest {

    @Test
    public void testDistribute() {
        UniformBucketDistributor bucketDistributor = new UniformBucketDistributor();
        //Validate empty coontrollers set.
        HashSet<String> controllers = new HashSet<>();
        Map<String, Set<Integer>> distribute = bucketDistributor.distribute(new HashMap<>(), controllers, 100);
        assertEquals(0, distribute.size());

        int bucketCount = 10;
        //Validate initialization.
        controllers.add("controller1");
        distribute = bucketDistributor.distribute(new HashMap<>(), controllers, bucketCount);
        assertEquals(1, distribute.size());
        validateContainerCount(distribute, controllers, bucketCount);

        //New controller added.
        controllers.add("controller2");
        distribute = bucketDistributor.distribute(distribute, controllers, bucketCount);
        assertEquals(2, distribute.size());
        validateContainerCount(distribute, controllers, bucketCount);

        //Add multiple controllers.
        controllers.add("controller3");
        controllers.add("controller4");
        distribute = bucketDistributor.distribute(distribute, controllers, bucketCount);
        assertEquals(4, distribute.size());
        validateContainerCount(distribute, controllers, bucketCount);

        //Remove controller.
        controllers.remove("controller2");
        distribute = bucketDistributor.distribute(distribute, controllers, bucketCount);
        assertEquals(3, distribute.size());
        validateContainerCount(distribute, controllers, bucketCount);

        //Add and remove multiple hosts.
        controllers.add("controller2");
        controllers.add("controller5");
        controllers.add("controller6");
        controllers.add("controller7");
        controllers.add("controller8");
        controllers.add("controller9");
        controllers.remove("controller1");
        controllers.remove("controller3");
        controllers.remove("controller4");
        distribute = bucketDistributor.distribute(distribute, controllers, bucketCount);
        assertEquals(6, distribute.size());
        validateContainerCount(distribute, controllers, bucketCount);

        //Remove multiple controllers.
        controllers.remove("controller2");
        controllers.remove("controller5");
        controllers.remove("controller6");
        distribute = bucketDistributor.distribute(distribute, controllers, bucketCount);
        assertEquals(3, distribute.size());
        validateContainerCount(distribute, controllers, bucketCount);

        //No changes.
        distribute = bucketDistributor.distribute(distribute, controllers, bucketCount);
        assertEquals(3, distribute.size());
        validateContainerCount(distribute, controllers, bucketCount);
    }


    private void validateContainerCount(Map<String, Set<Integer>> bucketMap, Set<String> controllers, int count) {
        long contCount = bucketMap.values().stream().map(m -> m.size()).reduce((a, b) -> a + b).get();
        assertTrue(contCount == count);

        Set<Integer> containersInMap = bucketMap.values().stream().
                                                   flatMap(m -> m.stream()).collect(Collectors.toSet());
        assertEquals(containersInMap,
                IntStream.range(0, count).boxed().collect(Collectors.toSet()));

        long maxContCount = bucketMap.entrySet().stream().map(
                m -> m.getValue().size()).max(Integer::compareTo).get();
        long minContCount = bucketMap.entrySet().stream().map(
                m -> m.getValue().size()).min(Integer::compareTo).get();
        assertTrue(maxContCount - minContCount <= 1);

        //Verify the controllers in the map matches the expected controllers.
        assertEquals(bucketMap.keySet(), controllers);
    }
}