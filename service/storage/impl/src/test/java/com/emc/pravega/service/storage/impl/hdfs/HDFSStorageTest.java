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
import com.emc.pravega.service.storage.SegmentHandle;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.mocks.StorageTestBase;
import lombok.Data;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Properties;
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

        return storage;
    }

    @Override
    protected SegmentHandle createInvalidHandle(String segmentName) {
        return new TestHandle(segmentName);
    }

    @Data
    private class TestHandle implements SegmentHandle {
        private final String segmentName;
    }
}
