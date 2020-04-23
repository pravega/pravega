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
import io.pravega.segmentstore.contracts.StreamingException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

public class RollingStorageMockTests {

    static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Test
    public void testNext() {
        RollingStorageMockTests.TestRollingStorageSegmentIterator testRollingStorageSegmentIterator = new
                RollingStorageMockTests.TestRollingStorageSegmentIterator(null, null,
                null);
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
