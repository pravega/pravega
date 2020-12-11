/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

import io.pravega.shared.health.impl.StatusAggregatorImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StatusAggregatorTests {

    List<Status> statuses = Arrays.asList(Status.UP, Status.UP, Status.DOWN);

    /**
     * Ensures that only a *majority* of Status results need indicate 'UP' (success) to reduce into a UP status.
     * The rule does not necessarily check for a 'UP' status majority, but rather if the majority of Status'
     * represent an 'alive' Status.
     */
    @Test
    public void testMajorityRule() {
        StatusAggregator aggregator = StatusAggregatorImpl.MAJORITY;
        Assert.assertEquals("The aggregator should return an UP status.",
                Status.UP,
                aggregator.aggregate(statuses));
        // Ensure it must be strictly greater than half.
        List<Status> list = new ArrayList<>(Arrays.asList(Status.DOWN));
        list.addAll(statuses);
        Assert.assertEquals("The aggregation should fail and return a DOWN status.",
                Status.DOWN,
                aggregator.aggregate(list));
    }

    /**
     * Makes sure that *all* Status results must be in agreement to reduce into a UP result.
     */
    @Test
    public void testUnanimousRule() {
        StatusAggregator aggregator = StatusAggregatorImpl.UNANIMOUS;
        Assert.assertEquals("The aggregator should return a DOWN status.",
                aggregator.aggregate(statuses),
                Status.DOWN);
    }

    /**
     * Will return an UP status as long as *any* of the individual Status results notes an UP status.
     */
    @Test
    public void testAnyRule() {
        StatusAggregator aggregator = StatusAggregatorImpl.ANY;
        Assert.assertEquals("The aggregator should return an UP status.",
                aggregator.aggregate(statuses),
                Status.UP);
    }

}
