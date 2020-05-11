/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.rolling;

import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamingException;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.test.common.AssertExtensions;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

public class RollingStorageSegmentIteratorMockTests {
    static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final SegmentRollingPolicy DEFAULT_ROLLING_POLICY = new SegmentRollingPolicy(100);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    /**
     * Tests the scenario when openHandle method throws StreamingException exception during the method under test execution.
     * @throws StreamingException openHandle method mocked to throw this exception.
     */
    @Test
    public void testNext() throws StreamingException {
        RollingStorageSegmentIteratorMockTests.TestRollingStorageSegmentIterator testRollingStorageSegmentIterator = new
                RollingStorageSegmentIteratorMockTests.TestRollingStorageSegmentIterator(null, null,
                null);
        val baseStorage = new InMemoryStorage();
        testRollingStorageSegmentIterator.instance = Mockito.spy(new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY));
        testRollingStorageSegmentIterator.current = StreamSegmentInformation.builder()
                .name("x$header")
                .build();
        Mockito.doThrow(mock(StreamSegmentException.class)).when(testRollingStorageSegmentIterator.instance).openHandle(anyString(), anyBoolean());
        AssertExtensions.assertThrows(NoSuchElementException.class, () -> testRollingStorageSegmentIterator.next());
    }

    /**
     * Tests when SegmentProperties is null during the method under test execution.
     */
    @Test
    public void testNextNullSegmentProperties() {
        RollingStorageSegmentIteratorMockTests.TestRollingStorageSegmentIterator testRollingStorageSegmentIterator = new
                RollingStorageSegmentIteratorMockTests.TestRollingStorageSegmentIterator(null, null,
                null);
        AssertExtensions.assertThrows(NoSuchElementException.class, () -> testRollingStorageSegmentIterator.next());
    }

    /**
     * Tests when Iterator for SegmentProperties is null.
     */
    @Test
    public void testHasNextNullIterator() {
        RollingStorageSegmentIteratorMockTests.TestRollingStorageSegmentIterator testRollingStorageSegmentIterator = new
                RollingStorageSegmentIteratorMockTests.TestRollingStorageSegmentIterator(null, null,
                null);
        boolean hasNext = testRollingStorageSegmentIterator.hasNext();
        Assert.assertFalse(hasNext);
    }

    /**
     * A derived class to mock a few methods called during method under test execution.
     */
    private static class TestRollingStorageSegmentIterator extends RollingStorage.RollingStorageSegmentIterator {
        public TestRollingStorageSegmentIterator(RollingStorage instance, Iterator<SegmentProperties> results,
                                                 java.util.function.Predicate<SegmentProperties> patternMatchPredicate) {
            super(instance, results, patternMatchPredicate);
        }
    }
}
