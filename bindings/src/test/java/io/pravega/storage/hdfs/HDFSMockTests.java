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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.NoSuchElementException;

public class HDFSMockTests {

    static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Test
    public void testNext() {
        HDFSMockTests.TestHDFSStorageSegmentIterator testHDFSStorageSegmentIterator = new
                HDFSMockTests.TestHDFSStorageSegmentIterator(null, null);
        boolean caughtException = false;
        try {
            testHDFSStorageSegmentIterator.next();
        } catch (NoSuchElementException e) {
            caughtException = true;
        }
        Assert.assertTrue(caughtException);
    }

    @Test
    public void testHasNext() {
        HDFSMockTests.TestHDFSStorageSegmentIterator testHDFSStorageSegmentIterator = new
                HDFSMockTests.TestHDFSStorageSegmentIterator(null, null);
        boolean hasNextValue = testHDFSStorageSegmentIterator.hasNext();
        Assert.assertFalse(hasNextValue);
    }

    /**
     * A derived class to mock a few methods called during method under test execution.
     */
    private static class TestHDFSStorageSegmentIterator extends HDFSStorage.HDFSSegmentIterator {
        public TestHDFSStorageSegmentIterator(RemoteIterator<FileStatus> results,
                               java.util.function.Predicate<FileStatus> patternMatchPredicate) {
            super(results, patternMatchPredicate);
        }

        protected boolean test(FileStatus fileStatus) throws IOException {
            throw new IOException();
        }

        protected boolean isSealed(Path path) throws FileNameFormatException {
            throw new FileNameFormatException("", "");
        }
    }
}
