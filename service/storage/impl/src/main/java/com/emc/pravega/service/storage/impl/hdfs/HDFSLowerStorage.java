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
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.storage.BadOffsetException;
import com.emc.pravega.service.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Slf4j
class HDFSLowerStorage implements Storage {
    private final Executor executor;
    private FileSystem fs;
    private Configuration conf;
    private final HDFSStorageConfig serviceBuilderConfig;

    public HDFSLowerStorage(HDFSStorageConfig serviceBuilderConfig, Executor executor) {
        this.serviceBuilderConfig = serviceBuilderConfig;
        this.executor = executor;
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


    SegmentProperties createSync(String streamSegmentName, Duration timeout) throws IOException {
            getFS().create(new Path(streamSegmentName),
                    new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE),
                    false,
                    0,
                    this.serviceBuilderConfig.getReplication(),
                    this.serviceBuilderConfig.getBlockSize(),
                    null).close();
            return new StreamSegmentInformation(streamSegmentName,
                    0,
                    false,
                    false,
                    new Date()
            );
    }

    SegmentProperties getStreamSegmentInfoSync(String streamSegmentName, Duration timeout) throws IOException {
        FileStatus[] status = getFS().globStatus(new Path(streamSegmentName));
        return new StreamSegmentInformation(streamSegmentName,
                status[0].getLen(),
                status[0].getPermission().getUserAction() == FsAction.READ,
                false,
                new Date(status[0].getModificationTime()));

    }


    @Override
    public CompletableFuture<Boolean> acquireLockForSegment(String streamSegmentName) {
        //Acquirelock is not implemented at the lower level of HDFS
        CompletableFuture<Boolean> retVal = new CompletableFuture<>();
        retVal.completeExceptionally(new IOException("Not Implemented"));
        return retVal;
    }

    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
        CompletableFuture<Void> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
                    try {
                        retVal.complete(writeSync(streamSegmentName, offset, length, data, timeout));
                    } catch (Exception e) {
                        retVal.completeExceptionally(e);
                    }
                },
                executor);
        return retVal;
    }



    private Void writeSync(String streamSegmentName, long offset, int length, InputStream data, Duration timeout) throws IOException, BadOffsetException {
        FSDataOutputStream stream = getFS().append(new Path(streamSegmentName));
        if (stream.getPos() != offset) {
            throw new BadOffsetException("Offset in the stream already has some data");
        }
        try {
            IOUtils.copyBytes(data, stream, length);
            stream.flush();
        } finally {
            stream.close();
        }
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

    SegmentProperties sealSync(String streamSegmentName, Duration timeout) throws IOException {
        getFS().setPermission(
                new Path(streamSegmentName),
                new FsPermission(FsAction.READ,
                        FsAction.NONE,
                        FsAction.NONE
                )
        );
        return this.getStreamSegmentInfoSync(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset, String sourceStreamSegmentName, Duration timeout) {
        CompletableFuture<Void> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                retVal.complete(concatSync(targetStreamSegmentName, offset, sourceStreamSegmentName, timeout));
            } catch (Exception e) {
                retVal.completeExceptionally(e);
            }
        }, executor);
        return retVal;
    }


    Void concatSync(String targetStreamSegmentName, long offset, String sourceStreamSegmentName, Duration timeout) throws IOException, BadOffsetException {
        if( getFS().globStatus(new Path(targetStreamSegmentName))[0].getLen() != offset )
            throw new BadOffsetException(targetStreamSegmentName + " has more data than" + offset);
        getFS().concat(new Path(targetStreamSegmentName),
                new Path[]{
                        new Path(targetStreamSegmentName),
                        new Path(sourceStreamSegmentName)
                });
        return null;
    }

    @Override
    public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
        CompletableFuture<Void> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                retVal.complete(deleteSync(streamSegmentName, timeout));
            } catch (IOException e) {
                retVal.completeExceptionally(e);
            }
        }, executor);
        return retVal;
    }

    @Override
    public void close() {
        try {
            this.getFS().close();
        } catch (IOException e) {
            log.debug("Could not close the fs. The error is", e);
        }

    }

    @Override
    public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        CompletableFuture<Integer> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                retVal.complete(readSync(streamSegmentName, offset, buffer, bufferOffset, length, timeout));
            } catch (IOException e) {
                retVal.completeExceptionally(e);
            }
        }, executor);
        return retVal;
    }

    /**
     * Finds the file containing the given offset for the given segment.
     * Reads from that file.
     */
    private Integer readSync(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) throws IOException {
        FSDataInputStream stream = getFS().open(new Path(streamSegmentName));
        return stream.read(offset,
                buffer, bufferOffset, length);
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        CompletableFuture<Boolean> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
                    try {
                        retVal.complete(existsSync(streamSegmentName, timeout));
                    } catch (IOException e) {
                        retVal.completeExceptionally(e);
                    }
                },
                executor);
        return retVal;
    }

    private Boolean existsSync(String streamSegmentName, Duration timeout) throws IOException {
        return getFS().exists(new Path(streamSegmentName));
    }

    FileSystem getFS() throws IOException {
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

    public Void deleteSync(String name, Duration timeout) throws IOException {
        getFS().delete(new Path(name), false);
        return null;
    }
}
