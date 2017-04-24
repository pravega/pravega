/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.service.storage.impl.hdfs;

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
