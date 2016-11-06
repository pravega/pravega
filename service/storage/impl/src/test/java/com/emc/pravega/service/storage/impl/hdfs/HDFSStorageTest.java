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

import com.emc.pravega.service.storage.SegmentHandle;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.mocks.StorageTestBase;
import lombok.Data;
import org.junit.Ignore;

import java.util.Properties;
import java.util.concurrent.ForkJoinPool;

/**
 * Unit tests for HDFSStorage.
 */
@Ignore
public class HDFSStorageTest extends StorageTestBase {
    @Override
    protected Storage createStorage() {
        Properties prop = new Properties();
        prop.setProperty("hdfs.fs.default.name", "localhost:9000");
        prop.setProperty("hdfs.hdfsRoot", "");
        prop.setProperty("hdfs.pravegaId", "0");
        prop.setProperty("hdfs.replication", "1");
        prop.setProperty("hdfs.blockSize", "1048576");
        HDFSStorageConfig config = new HDFSStorageConfig(prop);
        return new HDFSStorage(config, ForkJoinPool.commonPool());
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
