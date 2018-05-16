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

import com.google.common.collect.ImmutableMap;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the SegmentStateMapper class.
 */
public class SegmentStateMapperTests extends ThreadPooledTestSuite {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    /**
     * Tests the ability to retrieve Segment Info from Storage (base info and its State).
     */
    @Test
    public void testGetSegmentInfoFromStorage() {
        @Cleanup
        val s = createStorage();
        val stateStore = new InMemoryStateStore();
        val m = new SegmentStateMapper(stateStore, s);

        // Save some state.
        val info = s.create("s", TIMEOUT)
                .thenCompose(si -> s.openWrite(si.getName()))
                .thenCompose(handle -> s.write(handle, 0, new ByteArrayInputStream(new byte[10]), 10, TIMEOUT)
                        .thenCompose(v -> s.seal(handle, TIMEOUT))
                        .thenCompose(v -> s.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT))).join();
        val expectedInfo = StreamSegmentInformation.from(info).startOffset(info.getLength() / 2).build();
        val allAttributes = createAttributes();
        val expectedAttributes = Attributes.getCoreNonNullAttributes(allAttributes);
        val attributes = allAttributes.entrySet().stream()
                .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.Replace, e.getValue()))
                .collect(Collectors.toList());
        m.saveState(expectedInfo, attributes, TIMEOUT).join();

        // Retrieve the state and verify it.
        val actual = m.getSegmentInfoFromStorage(expectedInfo.getName(), TIMEOUT).join();
        Assert.assertEquals("Unexpected Name.", expectedInfo.getName(), actual.getName());
        Assert.assertEquals("Unexpected Length.", expectedInfo.getLength(), actual.getLength());
        Assert.assertEquals("Unexpected Sealed status.", expectedInfo.isSealed(), actual.isSealed());
        Assert.assertEquals("Unexpected Start Offset.", expectedInfo.getStartOffset(), actual.getStartOffset());
        AssertExtensions.assertMapEquals("Unexpected Attributes.", expectedAttributes, actual.getAttributes());
    }

    /**
     * Tests the ability to Save and later retrieve a State from the State Store.
     */
    @Test
    public void testStateOperations() {
        @Cleanup
        val s = createStorage();
        val stateStore = new InMemoryStateStore();
        val m = new SegmentStateMapper(stateStore, s);

        // Save some state.
        val sp = StreamSegmentInformation.builder().name("s").length(10).startOffset(4).sealed(true).build();
        val allAttributes = createAttributes();
        val expectedAttributes = Attributes.getCoreNonNullAttributes(allAttributes);
        val attributes = allAttributes.entrySet().stream()
                .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.Replace, e.getValue()))
                .collect(Collectors.toList());
        m.saveState(sp, attributes, TIMEOUT).join();

        // Get raw state.
        val state = m.getState(sp.getName(), TIMEOUT).join();
        Assert.assertEquals("Unexpected SegmentName.", sp.getName(), state.getSegmentName());
        Assert.assertEquals("Unexpected StartOffset.", sp.getStartOffset(), state.getStartOffset());
        AssertExtensions.assertMapEquals("Unexpected Attributes.", expectedAttributes, state.getAttributes());

        // Combine a state with raw properties.
        val rawSp = StreamSegmentInformation.builder().name(sp.getName()).length(10).startOffset(0).sealed(true).build();
        val si = m.attachState(rawSp, TIMEOUT).join();
        Assert.assertEquals("Unexpected StartOffset.", sp.getStartOffset(), si.getProperties().getStartOffset());
        AssertExtensions.assertMapEquals("Unexpected Attributes.", expectedAttributes, si.getProperties().getAttributes());
    }

    private Map<UUID, Long> createAttributes() {
        return ImmutableMap.of(Attributes.EVENT_COUNT, 100L, UUID.randomUUID(), 200L);
    }

    private Storage createStorage() {
        val factory = new InMemoryStorageFactory(executorService());
        return factory.createStorageAdapter();
    }
}
