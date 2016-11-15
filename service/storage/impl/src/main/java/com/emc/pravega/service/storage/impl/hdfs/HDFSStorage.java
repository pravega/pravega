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

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * higher level HDFSStorage which does lock implementation based on file permissions.
 * Each segment is represented by a file with pattern <segment-name>_<owner_host_id>
 * Owner_host_id is optional and means that the segment is owned by the Pravega host of the given id.
 * <p>
 * When ever ownership change happens, the new node renames the file representing the segment to <segment-name>_<owner_host_id>.
 * This is done by the acquireLockForSegment call.
 *
 * When a segment is sealed, it is renamed to its absolute name "segment-name" and marked as read-only.
 */
@Slf4j
public class HDFSStorage implements Storage {

    private final Executor executor;
    private final HDFSStorageConfig serviceBuilderConfig;
    private final AtomicBoolean closed;
    private FileSystem fileSystem;


    public HDFSStorage(HDFSStorageConfig serviceBuilderConfig, Executor executor) {
        Preconditions.checkNotNull(serviceBuilderConfig, "serviceBuilderConfig");
        Preconditions.checkNotNull(executor, "executor");
        this.serviceBuilderConfig = serviceBuilderConfig;
        this.executor = executor;
        this.storage = new HDFSLowerStorage(serviceBuilderConfig, executor);
    }


    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return FutureHelpers.runAsyncTranslateException(() -> createSync(streamSegmentName, timeout),
                e -> HDFSExceptionHelpers.translateFromException(streamSegmentName, e),
                this.executor);
    }


    SegmentProperties createSync(String streamSegmentName, Duration timeout) throws IOException {
        fileSystem.create(new Path(this.getOwnedSegmentFullPath(streamSegmentName)),
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

    /**
     * Utility function to change the name in the SegmentProperties from the last file
     * to the segment name
     *
     * @param properties        Properties representing the segment name
     * @param streamSegmentName The name of the segment
     */
    private SegmentProperties changeNameInSegmentProperties(SegmentProperties properties, String streamSegmentName) {
        return new StreamSegmentInformation(streamSegmentName,
                properties.getLength(),
                properties.isSealed(),
                properties.isDeleted(),
                properties.getLastModified());
    }

    /**
     * Utility function to get the full name of the file representing the segment which is locked by the current
     * Pravega host.
     */
    private String getOwnedSegmentFullPath(String streamSegmentName) {
        return getCommonPartOfName(streamSegmentName) + "_" + this.serviceBuilderConfig.getPravegaID();
    }

    /**
     * Utility function to get the wildcard path string which represents all the files that represent
     * current segment
     */
    private FileStatus[] getStreamSegmentNameWildCard(String streamSegmentName) throws IOException {
        return fileSystem.globStatus(new Path(getCommonPartOfName(streamSegmentName) + "_" + "[0-9]*"));
    }

    private String getCommonPartOfName(String streamSegmentName) {
        return serviceBuilderConfig.getHdfsRoot() + "/" + streamSegmentName;
    }

    @Override
    public CompletableFuture<Void> open(String streamSegmentName) {
        return FutureHelpers.runAsyncTranslateException(() -> {
                    openSync(streamSegmentName);
                    return null;
                },
                e -> HDFSExceptionHelpers.translateFromException(streamSegmentName, e),
                executor);
    }

    /**
     * Algorithm to take over the ownership of a segment.
     * <p>
     * List the file that represents the segment. This may be owned by some other node.
     * Rename the file to the current node.
     *
     */
    private void openSync(String streamSegmentName) throws IOException, StreamSegmentNotExistsException {

        FileStatus[] statuses = this.getStreamSegmentNameWildCard(streamSegmentName);

        if (statuses.length != 1) {
            throw new StreamSegmentNotExistsException(streamSegmentName);
        }
        fileSystem.rename(statuses[0].getPath(), new Path(this.getOwnedSegmentFullPath(streamSegmentName)));
    }

    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
        return FutureHelpers.runAsyncTranslateException(
                () ->  writeSync(streamSegmentName, offset, length, data, timeout),
                e -> HDFSExceptionHelpers.translateFromException(streamSegmentName, e),
                this.executor);
    }



    private Void writeSync(String streamSegmentName, long offset, int length, InputStream data, Duration timeout)
            throws BadOffsetException, IOException {
        try (FSDataOutputStream stream = fileSystem.append(new Path(this.getOwnedSegmentFullPath(streamSegmentName)))) {
            if (stream.getPos() != offset) {
                throw new BadOffsetException(streamSegmentName, offset, stream.getPos());
            }
            IOUtils.copyBytes(data, stream, length);
            stream.flush();
        }
        return null;
    }



    @Override
    public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
        return FutureHelpers.runAsyncTranslateException(
                () -> sealSync(streamSegmentName, timeout),
                e -> HDFSExceptionHelpers.translateFromException(streamSegmentName, e),
                executor);
    }

    SegmentProperties sealSync(String streamSegmentName, Duration timeout) throws IOException {
        fileSystem.setPermission(
                new Path(this.getOwnedSegmentFullPath(streamSegmentName)),
                new FsPermission(
                        FsAction.READ,
                        FsAction.READ,
                        FsAction.READ
                )
        );
        return this.getStreamSegmentInfoSync(streamSegmentName, timeout);
    }

    SegmentProperties getStreamSegmentInfoSync(String streamSegmentName, Duration timeout) throws IOException {
        FileStatus[] status = fileSystem.globStatus(new Path(this.getOwnedSegmentFullPath(streamSegmentName)));
        return new StreamSegmentInformation(streamSegmentName,
                status[0].getLen(),
                status[0].getPermission().getUserAction() == FsAction.READ,
                false,
                new Date(status[0].getModificationTime()));

    }

    @Override
    public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset, String sourceStreamSegmentName, Duration timeout) {
        return FutureHelpers.runAsyncTranslateException(
                () -> concatSync(targetStreamSegmentName, offset, sourceStreamSegmentName, timeout),
                e -> HDFSExceptionHelpers.translateFromException(targetStreamSegmentName, e),
                executor);
    }

    Void concatSync(String targetStreamSegmentName, long offset, String sourceStreamSegmentName, Duration timeout) throws IOException, BadOffsetException, StreamSegmentSealedException {
        FileStatus[] status = fileSystem.globStatus(new Path(this.getOwnedSegmentFullPath(targetStreamSegmentName)));
        if (status == null) {
            throw new FileNotFoundException(targetStreamSegmentName);
        }
        FileStatus[] sourceStatus = fileSystem.globStatus(new Path(this.getOwnedSegmentFullPath(sourceStreamSegmentName)));
        if (sourceStatus == null) {
            throw new FileNotFoundException(sourceStreamSegmentName);
        }
        if (sourceStatus[0].getPermission().getUserAction() != FsAction.READ) {
            throw new IllegalStateException(sourceStreamSegmentName);
        }
        if (status[0].getLen() != offset) {
            throw new BadOffsetException(targetStreamSegmentName, offset, status[0].getLen());
        }
        fileSystem.concat(new Path(this.getOwnedSegmentFullPath(targetStreamSegmentName)),
                new Path[]{
                        new Path(this.getOwnedSegmentFullPath(sourceStreamSegmentName))
                });
        return null;
    }


    @Override
    public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
        return FutureHelpers.runAsyncTranslateException(
                () -> deleteSync(streamSegmentName, timeout),
                e -> HDFSExceptionHelpers.translateFromException(streamSegmentName, e),
                executor);
    }


    public Void deleteSync(String name, Duration timeout) throws IOException {
        fileSystem.delete(new Path(this.getOwnedSegmentFullPath(name)), false);
        return null;
    }

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            if (this.fileSystem != null) {
                try {
                    this.fileSystem.close();
                    this.fileSystem = null;
                } catch (IOException e) {
                    log.warn("Could not close the HDFS filesystem: {}.", e);
                }
            }
        }
    }

    //endregion


    @Override
    public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return FutureHelpers.runAsyncTranslateException(
                () -> readSync(streamSegmentName, offset, buffer, bufferOffset, length, timeout),
                e -> HDFSExceptionHelpers.translateFromException(streamSegmentName, e),
                executor);
    }

    /**
     * Finds the file containing the given offset for the given segment.
     * Reads from that file.
     */
    private Integer readSync(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) throws IOException {
        if (offset < 0 || bufferOffset < 0 || length < 0 || buffer.length < bufferOffset+length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        FSDataInputStream stream = fileSystem.open(new Path(this.getOwnedSegmentFullPath(streamSegmentName)));
        int retVal = stream.read(offset,
                buffer, bufferOffset, length);
        if (retVal < 0) {
            // -1 is usually a code for invalid args; check to see if we were supplied with an offset that exceeds the length of the segment.
            long segLen = getStreamSegmentInfoSync(streamSegmentName, null).getLength();
            if (offset >= segLen) {
                throw new IllegalArgumentException(String.format("Read offset (%s) is beyond the length of the segment (%s).", offset, segLen));
            }
        }
        return retVal;
    }


    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return FutureHelpers.runAsyncTranslateException(
                () ->  this.getStreamSegmentInfoSync(streamSegmentName, timeout),
                e -> HDFSExceptionHelpers.translateFromException(streamSegmentName, e),
                executor);
    }




    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return FutureHelpers.runAsyncTranslateException(
                () -> existsSync(streamSegmentName, timeout),
                e -> HDFSExceptionHelpers.translateFromException(streamSegmentName, e),
                executor);
    }

    private Boolean existsSync(String streamSegmentName, Duration timeout) throws IOException {
        return fileSystem.exists(new Path(streamSegmentName));
    }

    /**
     * Initializes the HDFSStorage.
     *
     * @throws IOException If the initialization failed.
     */
    public void initialize() throws IOException {
        Preconditions.checkState(this.fileSystem == null, "HDFSStorage has already been initialized.");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", serviceBuilderConfig.getHDFSHostURL());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        this.fileSystem = FileSystem.get(conf);
        closed.set(false);
    }
}
