/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import io.pravega.common.util.AsyncMap;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Defines tests for a generic State Store (AsyncMap(String, SegmentState))
 */
public abstract class StateStoreTests extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final int ATTRIBUTE_COUNT = 10;
    private static final UUID CORE_ATTRIBUTE = Attributes.EVENT_COUNT;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    //region Test Definitions

    /**
     * Tests the get() method when there is no state.
     */
    @Test
    public void testGetNoState() throws Exception {
        final String segmentName = "foo";
        val ss = createStateStore();
        val state = ss.get(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNull("Not expecting any state for a segment with no state.", state);
    }

    /**
     * Tests the put()/get()/remove() methods.
     */
    @Test
    public void testPutGetRemove() throws Exception {
        final int segmentCount = 100;

        val ss = createStateStore();

        // Put and verify everything.
        ArrayList<String> segmentNames = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
            SegmentState original = createState(Integer.toString(i));
            segmentNames.add(original.getSegmentName());
            ss.put(original.getSegmentName(), original, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            val deserialized = ss.get(original.getSegmentName(), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Assert.assertEquals("Unexpected segment name.", original.getSegmentName(), deserialized.getSegmentName());
            val expectedAttributes = Attributes.getCoreNonNullAttributes(original.getAttributes());
            AssertExtensions.assertMapEquals("Unexpected attributes.", expectedAttributes, deserialized.getAttributes());
        }

        // Remove everything and verify it was removed.
        for (String segmentName : segmentNames) {
            ss.remove(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            val state = ss.get(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Assert.assertNull("Not expecting any state for a segment whose state was removed.", state);
        }
    }

    /**
     * Tests the put() method with an already existing key.
     */
    @Test
    public void testPutOverwrite() throws Exception {
        final String segmentName = "foo";
        final SegmentState state1 = createState(segmentName);
        final SegmentState state2 = createState(segmentName);
        val ss = createStateStore();

        ss.put(segmentName, state1, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        ss.put(segmentName, state2, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        val deserialized = ss.get(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected segment id", state2.getSegmentId(), deserialized.getSegmentId());
        Assert.assertEquals("Unexpected segment name.", state2.getSegmentName(), deserialized.getSegmentName());
        val expectedAttributes = Attributes.getCoreNonNullAttributes(state2.getAttributes());
        AssertExtensions.assertMapEquals("Unexpected attributes.", expectedAttributes, deserialized.getAttributes());
    }

    //endregion

    protected abstract AsyncMap<String, SegmentState> createStateStore();

    private SegmentState createState(String segmentName) {
        HashMap<UUID, Long> attributes = new HashMap<>();

        // One Core Attribute, and the rest are all Extended Attributes.
        attributes.put(CORE_ATTRIBUTE, (long) attributes.size());
        while (attributes.size() < ATTRIBUTE_COUNT) {
            attributes.put(UUID.randomUUID(), (long) attributes.size());
        }

        return new SegmentState(segmentName.hashCode(), StreamSegmentInformation.builder().name(segmentName).attributes(attributes).build());
    }

    //region InMemoryStateStoreTests

    /**
     * Unit tests for the InMemoryStateStore class.
     */
    public static class InMemoryStateStoreTests extends StateStoreTests {
        @Override
        protected AsyncMap<String, SegmentState> createStateStore() {
            return new InMemoryStateStore();
        }
    }

    //endregion
}
