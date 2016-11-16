/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.emc.pravega.common.io.FileHelpers;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.mocks.StorageTestBase;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

/**
 * Unit tests for HDFSStorage.
 */
public class HDFSStorageTest extends StorageTestBase {
    private static File baseDir = null;
    private static MiniDFSCluster hdfsCluster = null;

    @org.junit.BeforeClass
    public static void setUp() throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.OFF);
        //context.reset();

        baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        conf.setBoolean("dfs.permissions.enabled", true);
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
    }

    @org.junit.AfterClass
    public static void tearDown() {
        if (hdfsCluster != null) {
            hdfsCluster.shutdown();
            hdfsCluster = null;
            FileHelpers.deleteFileOrDirectory(baseDir);
            baseDir = null;
        }
    }

    @Override
    protected Storage createStorage() {
        Properties prop = new Properties();
        prop.setProperty("hdfs.fs.default.name", String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort()));
        prop.setProperty("hdfs.hdfsRoot", "");
        prop.setProperty("hdfs.pravegaId", "0");
        prop.setProperty("hdfs.replication", "1");
        prop.setProperty("hdfs.blockSize", "1048576");
        HDFSStorageConfig config = new HDFSStorageConfig(prop);
        val storage = new HDFSStorage(config, ForkJoinPool.commonPool());
        try {
            storage.initialize();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        return new MiniClusterPermFixer(storage);
    }

    @Override
    protected String createInvalidHandle(String segmentName) {
        return segmentName + "_invalid";
    }

    /**
     * Wrapper for a storage class which handles the ACL behavior of MiniDFSCluster.
     * This keeps track of the sealed segments and throws error when a write is attempted on a segment.
     **/
    private class MiniClusterPermFixer implements Storage {
        private final HDFSStorage storage;
        private ArrayList<String> sealedList;

        public MiniClusterPermFixer(HDFSStorage storage) {
            sealedList = new ArrayList<>();
            this.storage = storage;
        }

        @Override
        public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
            return storage.create(streamSegmentName, timeout);
        }

        @Override
        public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length,
                                             Duration timeout) {

            if (sealedList.contains(streamSegmentName)) {
                CompletableFuture<Void> retVal = new CompletableFuture<>();
                retVal.completeExceptionally(new StreamSegmentSealedException(streamSegmentName));
                return retVal;
            }
            return storage.write(streamSegmentName, offset, data, length, timeout);
        }

        @Override
        public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
            addToSealedList(streamSegmentName);
            return storage.seal(streamSegmentName, timeout);
        }

        private void addToSealedList(String streamSegmentName) {
            sealedList.add(streamSegmentName);
        }

        @Override
        public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset, String
                sourceStreamSegmentName, Duration timeout) {
            return storage.concat(targetStreamSegmentName, offset, sourceStreamSegmentName, timeout);
        }

        @Override
        public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
            return storage.delete(streamSegmentName, timeout);
        }

        @Override
        public void close() {

        }

        @Override
        public CompletableFuture<Void> open(String streamSegmentName) {
            return storage.open(streamSegmentName);
        }

        @Override
        public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int
                bufferOffset, int length, Duration timeout) {
            return storage.read(streamSegmentName, offset, buffer, bufferOffset, length, timeout);
        }

        @Override
        public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
            return storage.getStreamSegmentInfo(streamSegmentName, timeout);
        }

        @Override
        public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
            return storage.exists(streamSegmentName, timeout);
        }
    }
}
