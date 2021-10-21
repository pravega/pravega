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
package io.pravega.local;

import io.pravega.common.Exceptions;
import io.pravega.common.io.FileHelpers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class LocalHDFSEmulator implements AutoCloseable {
    private File baseDir = null;
    private MiniDFSCluster hdfsCluster = null;
    private final String baseDirName;

    private LocalHDFSEmulator(String baseDirName) {
        Exceptions.checkNotNullOrEmpty(baseDirName, "baseDirName");
        this.baseDirName = baseDirName;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public void start() throws IOException {
        baseDir = Files.createTempDirectory(baseDirName).toFile().getAbsoluteFile();
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        conf.setBoolean("dfs.permissions.enabled", true);
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
    }

    @Override
    public void close() {
        if (hdfsCluster != null) {
            hdfsCluster.shutdown();
            hdfsCluster = null;
            FileHelpers.deleteFileOrDirectory(baseDir);
            baseDir = null;
        }
    }

    public int getNameNodePort() {
        return hdfsCluster.getNameNodePort();
    }

    public static class Builder {
        private String baseDirName;

        public Builder baseDirName(String baseDir) {
            Exceptions.checkNotNullOrEmpty(baseDir, "baseDir");
            this.baseDirName = baseDir;
            return this;
        }

        public LocalHDFSEmulator build() {
            return new LocalHDFSEmulator(baseDirName);
        }
    }
}
