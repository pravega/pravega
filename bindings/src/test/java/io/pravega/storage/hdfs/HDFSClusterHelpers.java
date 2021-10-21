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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * Utilities for managing a mini HDFS Cluster.
 */
public final class HDFSClusterHelpers {
    /**
     * Creates a MiniDFSCluster at the given Path.
     *
     * @param path The path to create at.
     * @return A MiniDFSCluster.
     * @throws IOException If an Exception occurred.
     */
    public static MiniDFSCluster createMiniDFSCluster(String path) throws IOException {
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, path);
        conf.setBoolean("dfs.permissions.enabled", true);
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        return builder.build();
    }
}
