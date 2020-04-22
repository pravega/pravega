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

import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.NoSuchElementException;

public class HDFSMockTests {

    static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
    private File baseDir = null;
    private MiniDFSCluster hdfsCluster = null;
    private HDFSStorageConfig adapterConfig;
    private RemoteIterator<FileStatus> results;
    private java.util.function.Predicate<FileStatus> patternMatchPredicate;

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
        this.hdfsCluster = HDFSClusterHelpers.createMiniDFSCluster(this.baseDir.getAbsolutePath());
        this.adapterConfig = HDFSStorageConfig
                .builder()
                .with(HDFSStorageConfig.REPLICATION, 1)
                .with(HDFSStorageConfig.URL, String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort()))
                .build();
    }

    @After
    public void tearDown() {
        if (hdfsCluster != null) {
            hdfsCluster.shutdown();
            hdfsCluster = null;
            FileHelpers.deleteFileOrDirectory(baseDir);
            baseDir = null;
        }
    }

    @Test
    public void nextTest() {
        HDFSMockTests.TestHDFSStorage testHDFSStorage = new HDFSMockTests.TestHDFSStorage(adapterConfig, results,
                patternMatchPredicate);
        boolean caughtException = false;
        try {
            testHDFSStorage.next();
        } catch (NoSuchElementException e) {
            caughtException = true;
        }
        Assert.assertTrue(caughtException);
    }

    @Test
    public void hasNextTest() {
        HDFSMockTests.TestHDFSStorage testHDFSStorage = new HDFSMockTests.TestHDFSStorage(adapterConfig, results,
                patternMatchPredicate);
        boolean hasNextValue = testHDFSStorage.hasNext();
        Assert.assertFalse(hasNextValue);
    }

    /**
     * Test Class.
     */
    private static class TestHDFSStorage extends HDFSStorage.HDFSSegmentIterator {
        public TestHDFSStorage(HDFSStorageConfig hdfsStorageConfig, RemoteIterator<FileStatus> results,
                               java.util.function.Predicate<FileStatus> patternMatchPredicate) {
            new HDFSStorage(hdfsStorageConfig).super(results, patternMatchPredicate);
        }

        protected boolean test(FileStatus fileStatus) throws IOException{
            throw new IOException();
        }

        protected boolean isSealed(Path path) throws FileNameFormatException {
            throw new FileNameFormatException("", "");
        }
    }

    /**
     * Test Class.
     */


}
