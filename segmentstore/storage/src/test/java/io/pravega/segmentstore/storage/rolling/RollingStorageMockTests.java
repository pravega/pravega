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

import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamingException;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

public class RollingStorageMockTests {
    private static final SegmentRollingPolicy DEFAULT_ROLLING_POLICY = new SegmentRollingPolicy(100);
    static final Duration TIMEOUT = Duration.ofSeconds(600);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Test
    public void testNext() throws StreamingException {
        RollingStorageMockTests.TestRollingStorageSegmentIterator testRollingStorageSegmentIterator = new
                RollingStorageMockTests.TestRollingStorageSegmentIterator(null, null,
                null);
        val baseStorage = new InMemoryStorage();
        testRollingStorageSegmentIterator.instance = Mockito.spy(new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY));
        testRollingStorageSegmentIterator.current = new SegmentProperties() {
            @Override
            public String getName() {
                return "x$header";
            }

            @Override
            public boolean isSealed() {
                return false;
            }

            @Override
            public boolean isDeleted() {
                return false;
            }

            @Override
            public ImmutableDate getLastModified() {
                return null;
            }

            @Override
            public long getStartOffset() {
                return 0;
            }

            @Override
            public long getLength() {
                return 0;
            }

            @Override
            public Map<UUID, Long> getAttributes() {
                return null;
            }
        };
        Mockito.doThrow(mock(StreamSegmentException.class)).when(testRollingStorageSegmentIterator.instance).openHandle(anyString(), anyBoolean());
        boolean caughtException = false;
        try {
            testRollingStorageSegmentIterator.next();
        } catch (NoSuchElementException e) {
            caughtException = true;
        }
        Assert.assertTrue(caughtException);
    }

    /**
     * A derived class to mock a few methods called during method under test execution.
     */
    private static class TestRollingStorageSegmentIterator extends RollingStorage.RollingStorageSegmentIterator {
        public TestRollingStorageSegmentIterator(RollingStorage instance, Iterator<SegmentProperties> results,
                                                 java.util.function.Predicate<SegmentProperties> patternMatchPredicate) {
            super(instance, results, patternMatchPredicate);
        }

        protected RollingSegmentHandle openHandle(String segmentName, boolean readOnly) throws StreamingException {
            throw new StreamingException("");
        }
    }
}
