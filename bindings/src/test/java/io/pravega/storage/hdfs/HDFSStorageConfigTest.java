/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.storage.hdfs;

import io.pravega.common.util.ConfigBuilder;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.*;

public class HDFSStorageConfigTest {


    /**
     * Test Construct Configuration
     */
    @Test
    public void testConstructHDFSConfig() {
        ConfigBuilder<HDFSStorageConfig> builder = HDFSStorageConfig.builder();
        builder.with(HDFSStorageConfig.URL, "localhost:8000")
            .with(HDFSStorageConfig.ROOT, "hdfsOtherRoot")
            .with(HDFSStorageConfig.IMPLEMENTATION, "org.apache.hadoop.hdfs.AlternativeDistributedFileSystem")
            .with(HDFSStorageConfig.REPLICATION, 4)
            .with(HDFSStorageConfig.BLOCK_SIZE, 128 * 128)
            .with(HDFSStorageConfig.REPLACE_DATANODES_ON_FAILURE, false);
        HDFSStorageConfig config = builder.build();
        assertEquals("localhost:8000", config.getHdfsHostURL());
        assertEquals("hdfsOtherRoot", config.getHdfsRoot());
        assertEquals("org.apache.hadoop.hdfs.AlternativeDistributedFileSystem", config.getHdfsImpl());
        assertEquals(4, config.getReplication());
        assertEquals(128 * 128, config.getBlockSize());
        assertFalse(config.isReplaceDataNodesOnFailure());
    }
}