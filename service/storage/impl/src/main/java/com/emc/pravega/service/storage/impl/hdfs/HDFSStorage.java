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

import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.server.StreamSegmentInformation;
import com.emc.pravega.service.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Slf4j
class HDFSStorage implements Storage {

    private final Executor executor;
    private final HDFSStorageConfig serviceBuilderConfig;
    private Configuration conf;
    private FileSystem fs;


    public HDFSStorage(HDFSStorageConfig serviceBuilderConfig, Executor executor) {
        this.serviceBuilderConfig = serviceBuilderConfig;
        this.executor  = executor;
        conf = null;
        fs = null;

    }

    private FileSystem getFS() throws IOException {
        if (fs == null) {
            conf = createFromConf(serviceBuilderConfig);
            fs = FileSystem.get(conf);
        }
        return fs;

    }

    private Configuration createFromConf(HDFSStorageConfig serviceBuilderConfig) {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", serviceBuilderConfig.getHDFSHostURL());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return conf;
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        CompletableFuture<SegmentProperties> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
                    try {
                        retVal.complete(createSync(streamSegmentName, timeout));
                    } catch (IOException e) {
                        retVal.completeExceptionally(e);
                    }
                },
                executor);
        return retVal;
    }

    private SegmentProperties createSync(String streamSegmentName, Duration timeout) throws IOException {
        if (!getFS().exists(new Path(getFullStreamSegmentPath(streamSegmentName))) &&
                !getFS().exists(new Path(getOwnedFullStreamSegmentPath(streamSegmentName)))) {

            //Create owned path
            getFS().create(new Path(getOwnedFullStreamSegmentPath(streamSegmentName)),
                    new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE),
                    false,
                    0,
                    this.serviceBuilderConfig.getReplication(),
                    this.serviceBuilderConfig.getBlockSize(),
                    null);
            return this.getStreamSegmentInfoSync(streamSegmentName, timeout);
        } else throw new IOException("Segment already exists");
    }

    private String getFullStreamSegmentPath(String streamSegmentName) {
        return serviceBuilderConfig.getHdfsRoot() + "/" + streamSegmentName;
    }

    private String getOwnedFullStreamSegmentPath(String streamSegmentName) {
        return serviceBuilderConfig.getHdfsRoot() + "/" + streamSegmentName + "_" + serviceBuilderConfig.getPravegaID();
    }


    @Override
    public CompletableFuture<Boolean> acquireLockForSegment(String streamSegmentName) {

        return CompletableFuture.supplyAsync(() -> acquireLockForSegmentSync(streamSegmentName),
                executor);
    }

    private Boolean acquireLockForSegmentSync(String streamSegmentName) {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> releaseLockForSegment(String streamSegmentName) {
        return CompletableFuture.supplyAsync(() -> {
                    return releaseLockForSegmentSync(streamSegmentName);
        }, executor);
    }

    private Boolean releaseLockForSegmentSync(String streamSegmentName) {
        return false;
    }

    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
        CompletableFuture<Void> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
                    try {
                        retVal.complete(writeSync(streamSegmentName, offset, data, timeout));
                    } catch (IOException e) {
                        retVal.completeExceptionally(e);
                    }
                },
                executor);
        return retVal;
    }

    private Void writeSync(String streamSegmentName, long offset, InputStream data, Duration timeout) throws IOException {
        //TODO: Write the ownership check and change logic
        fs.append(new Path(getOwnedFullStreamSegmentPath(streamSegmentName)));
        return null;
    }

    @Override
    public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
        CompletableFuture<SegmentProperties> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
                    try {
                        retVal.complete(sealSync(streamSegmentName, timeout));
                    } catch (IOException e) {
                        retVal.completeExceptionally(e);
                    }
                },
                executor);
        return retVal;
    }

    private SegmentProperties sealSync(String streamSegmentName, Duration timeout) throws IOException {
        getFS().setPermission(
                new Path(this.getOwnedFullStreamSegmentPath(streamSegmentName)),
                new FsPermission(FsAction.READ,
                        FsAction.NONE,
                        FsAction.NONE
                        )
        );
        return this.getStreamSegmentInfoSync(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<Void> concat(String targetStreamSegmentName, String sourceStreamSegmentName, Duration timeout) {
        CompletableFuture<Void> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
                    retVal.complete(concatSync(targetStreamSegmentName, sourceStreamSegmentName, timeout));
        }, executor);
        return retVal;
    }

    private Void concatSync(String targetStreamSegmentName, String sourceStreamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
        CompletableFuture<Void> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
                    retVal.complete(deleteSync(streamSegmentName, timeout));
        }, executor);
        return retVal;
    }

    private Void deleteSync(String streamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        CompletableFuture<Integer> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> retVal.complete(readSync(streamSegmentName, offset, buffer, bufferOffset, length, timeout)), executor);
        return retVal;
    }

    private Integer readSync(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return 0;
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        CompletableFuture<SegmentProperties> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
                    try {
                        retVal.complete(getStreamSegmentInfoSync(streamSegmentName, timeout));
                    } catch (IOException e) {
                        retVal.completeExceptionally(e);
                    }
                },
                executor);
        return retVal;
    }

    private SegmentProperties getStreamSegmentInfoSync(String streamSegmentName, Duration timeout) throws IOException {
        FileStatus status;
        try {
            status = fs.getFileStatus(new Path(this.getFullStreamSegmentPath(streamSegmentName)));
        } catch (FileNotFoundException e) {
            status = fs.getFileStatus(new Path(this.getOwnedFullStreamSegmentPath(streamSegmentName)));
        }
        return convertFromStatusToProperties(status, streamSegmentName);
    }

    private SegmentProperties convertFromStatusToProperties(FileStatus status, String streamSegmentName) {
        return new StreamSegmentInformation(
                streamSegmentName,
                status.getLen(),
                status.getPermission().getUserAction().equals(FsAction.READ),
                false,
                new Date(status.getModificationTime())
        );
    }
}
