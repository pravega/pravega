/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.hdfs;

import io.pravega.test.common.AssertExtensions;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import org.mockito.Mockito;

public class HDFSSegmentIteratorMockTests {
    static final Duration TIMEOUT = Duration.ofSeconds(30);
    static final java.util.function.Predicate<FileStatus> PREDICATE = new Predicate<FileStatus>() {
        @Override
        public boolean test(FileStatus fileStatus) {
            return true;
        }
    };

    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
    private RemoteIterator<FileStatus> results;

    @Before
    public void setUp() throws Exception {
        this.results = Mockito.spy(new RemoteIterator<FileStatus>() {
            // All methods defined here are mocked to return required values.
            @Override
            public boolean hasNext() throws IOException {
                return true;
            }

            @Override
            public FileStatus next() throws IOException {
                FileStatus fileStatus = new FileStatus();
                org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/testmock"); // Wrong format of path
                fileStatus.setPath(path);
                return fileStatus;
            }
        });
    }

    /**
     * Tests the exception handling capacity of hasNext() method.
     * @throws IOException RemoteIterator's hasNext() method was mocked to throw IOException.
     */
    @Test
    public void testHasNextIOException() throws IOException {
        HDFSSegmentIteratorMockTests.TestHDFSStorageSegmentIterator testHDFSStorageSegmentIterator = new
                HDFSSegmentIteratorMockTests.TestHDFSStorageSegmentIterator(this.results, null);
        Mockito.doThrow(new IOException()).when(this.results).hasNext();
        boolean hasNextValue = testHDFSStorageSegmentIterator.hasNext();
        Assert.assertFalse(hasNextValue);
    }

    /**
     * Tests the case when patternMatchPredicate.test() method returns true during the method under test execution.
     */
    @Test
    public void testHasNextPatternMatchPredicateReturnsTrue() {
        HDFSSegmentIteratorMockTests.TestHDFSStorageSegmentIterator testHDFSStorageSegmentIterator = new
                HDFSSegmentIteratorMockTests.TestHDFSStorageSegmentIterator(this.results, this.PREDICATE);
        boolean hasNextValue = testHDFSStorageSegmentIterator.hasNext();
        Assert.assertTrue(hasNextValue);
    }

    /**
     * Tests the scenario when RemoteIterator of FileStatus objects is null.
     */
    @Test
    public void testHasNextNullRemoteIterator() {
        HDFSSegmentIteratorMockTests.TestHDFSStorageSegmentIterator testHDFSStorageSegmentIterator = new
                HDFSSegmentIteratorMockTests.TestHDFSStorageSegmentIterator(null, null);
        boolean hasNextValue = testHDFSStorageSegmentIterator.hasNext();
        Assert.assertFalse(hasNextValue);
    }

    /**
     * Tests the scenario when FileNameFormatException is thrown during the next() method execution.
     */
    @Test
    public void testNextFileNameFormatException() {
        HDFSSegmentIteratorMockTests.TestHDFSStorageSegmentIterator testHDFSStorageSegmentIterator = Mockito.spy(new
                HDFSSegmentIteratorMockTests.TestHDFSStorageSegmentIterator(this.results, this.PREDICATE));
        AssertExtensions.assertThrows(NoSuchElementException.class, () -> testHDFSStorageSegmentIterator.next());
    }

    /**
     * Tests the scenario when hasNext method returns false during the method under the test execution.
     */
    @Test
    public void testNextWhenHasNextReturnsFalse() {
        HDFSSegmentIteratorMockTests.TestHDFSStorageSegmentIterator testHDFSStorageSegmentIterator = new
                HDFSSegmentIteratorMockTests.TestHDFSStorageSegmentIterator(null, null);
        AssertExtensions.assertThrows(NoSuchElementException.class, () -> testHDFSStorageSegmentIterator.next());
    }

    /**
     * A derived class to mock a few methods called during method under test execution.
     */
    private static class TestHDFSStorageSegmentIterator extends HDFSStorage.HDFSSegmentIterator {
        public TestHDFSStorageSegmentIterator(RemoteIterator<FileStatus> results,
                                              java.util.function.Predicate<FileStatus> patternMatchPredicate) {
            super(results, patternMatchPredicate);
        }
    }
}
