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
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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
    private static final Duration TIMEOUT = Duration.ofSeconds(10000);
    private static final int ATTRIBUTE_COUNT = 10;
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
            AssertExtensions.assertMapEquals("Unexpected attributes.", original.getAttributes(), deserialized.getAttributes());
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
        AssertExtensions.assertMapEquals("Unexpected attributes.", state2.getAttributes(), deserialized.getAttributes());
        for (Map.Entry<UUID, Long> a : state1.getAttributes().entrySet()) {
            Assert.assertFalse("Overwritten attribute found in deserialized state.", deserialized.getAttributes().containsKey(a.getKey()));
        }
    }

    /**
     * Tests the get() after a corrupt put().
     */
    @Test
    public void testGetAfterCorruptPut() throws Exception {
        final String segmentName = "foo";
        final SegmentState state1 = createState(segmentName);
        val ss = createStateStore();

        ss.put(segmentName, state1, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        this.emptySegment(segmentName);

        val deserialized = ss.get(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Assert.assertNull("Corrupt state should be discarded", deserialized);
    }
    //endregion

    protected abstract AsyncMap<String, SegmentState> createStateStore();

    private SegmentState createState(String segmentName) {
        HashMap<UUID, Long> attributes = new HashMap<>();
        for (int i = 0; i < ATTRIBUTE_COUNT; i++) {
            attributes.put(UUID.randomUUID(), (long) i);
        }

        return new SegmentState(segmentName.hashCode(),
                new StreamSegmentInformation(segmentName, 0, false, false, attributes, new ImmutableDate()));
    }

    public abstract void emptySegment(String segmentName);

    //region InMemoryStateStoreTests

    /**
     * Unit tests for the InMemoryStateStore class.
     */
    public static class InMemoryStateStoreTests extends StateStoreTests {
        private InMemoryStateStore stateStore = null;

        @Override
        protected AsyncMap<String, SegmentState> createStateStore() {
            stateStore = new InMemoryStateStore();
            return stateStore;
        }

        @Override
        public void emptySegment(String segmentName) {
            stateStore.remove(segmentName, null);
        }
    }

    //endregion
}
