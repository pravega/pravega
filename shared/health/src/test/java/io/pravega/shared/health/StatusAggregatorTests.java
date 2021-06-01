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
package io.pravega.shared.health;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StatusAggregatorTests {

    private static final List<Status> STATUSES = Arrays.asList(Status.UP, Status.UP, Status.DOWN);

    /**
     * Ensures that only a *majority* of Status results need indicate 'UP' (success) to reduce into a UP status.
     * The rule does not necessarily check for a 'UP' status majority, but rather if the majority of Status'
     * represent an 'alive' Status.
     */
    @Test
    public void testMajorityRule() {
        StatusAggregator aggregator = StatusAggregator.MAJORITY;
        Assert.assertEquals("The aggregator should return an UP status.",
                Status.UP,
                StatusAggregator.aggregate(aggregator, STATUSES));
        // Ensure it must be strictly greater than half.
        List<Status> list = new ArrayList<>(Arrays.asList(Status.DOWN));
        list.addAll(STATUSES);
        Assert.assertEquals("The aggregation should fail and return a DOWN status.",
                Status.DOWN,
                StatusAggregator.aggregate(aggregator, list));
    }

    /**
     * Makes sure that *all* Status results must be in agreement to reduce into a UP result.
     */
    @Test
    public void testUnanimousRule() {
        StatusAggregator aggregator = StatusAggregator.UNANIMOUS;
        Assert.assertEquals("The aggregator should return a DOWN status.",
                StatusAggregator.aggregate(aggregator, STATUSES),
                Status.DOWN);
    }

    /**
     * Will return an UP status as long as *any* of the individual Status results notes an UP status.
     */
    @Test
    public void testAnyRule() {
        StatusAggregator aggregator = StatusAggregator.ANY;
        Assert.assertEquals("The aggregator should return an UP status.",
                StatusAggregator.aggregate(aggregator, STATUSES),
                Status.UP);
    }

}
