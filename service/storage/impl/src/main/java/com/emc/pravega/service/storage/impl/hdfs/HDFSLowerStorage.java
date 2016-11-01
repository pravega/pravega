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

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.function.RunnableWithException;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.contracts.StreamingException;
import com.emc.pravega.service.storage.SegmentHandle;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageException;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
class HDFSLowerStorage implements Storage {
    // region Members
    private final Executor executor;
    private FileSystem fs;
    private Configuration conf;
    private final HDFSStorageConfig serviceBuilderConfig;
    private AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSLowerStorage class.
     *
     * @param serviceBuilderConfig Configuration for the storage.
     * @param executor             An executor to use for async operations.
     */
    HDFSLowerStorage(HDFSStorageConfig serviceBuilderConfig, Executor executor) {
        Preconditions.checkNotNull(serviceBuilderConfig, "serviceBuilderConfig");
        Preconditions.checkNotNull(executor, "executor");
        this.serviceBuilderConfig = serviceBuilderConfig;
        this.executor = executor;
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            if (this.fs != null) {
                try {
                    this.fs.close();
                } catch (IOException e) {
                    log.debug("Could not close the underlying filesystem: ", e);
                }
            }
        }
    }

    //endregion

    //region Storage Implementation

    @Override
    public CompletableFuture<SegmentHandle> create(String streamSegmentName, Duration timeout) {
        return supplyAsync(() -> createSync(streamSegmentName), streamSegmentName);
    }

    @Override
    public CompletableFuture<Void> acquireLockForSegment(String streamSegmentName) {
        throw new UnsupportedOperationException("acquireLockForSegment is not supported in HDFSLowerStorage.");
    }

    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
        return runAsync(() -> writeSync(streamSegmentName, offset, length, data), streamSegmentName);
    }

    @Override
    public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
        return supplyAsync(() -> sealSync(streamSegmentName), streamSegmentName);
    }

    @Override
    public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset, String sourceStreamSegmentName, Duration timeout) {
        return runAsync(() -> concatSync(targetStreamSegmentName, offset, sourceStreamSegmentName), sourceStreamSegmentName);
    }

    @Override
    public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
        return runAsync(() -> deleteSync(streamSegmentName), streamSegmentName);
    }

    @Override
    public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return supplyAsync(() -> readSync(streamSegmentName, offset, buffer, bufferOffset, length), streamSegmentName);
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return supplyAsync(() -> getStreamSegmentInfoSync(streamSegmentName), streamSegmentName);
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return supplyAsync(() -> existsSync(streamSegmentName), streamSegmentName);
    }

    //endregion

    //region Sync operations

    private SegmentHandle createSync(String streamSegmentName) throws IOException {
        // Create an immediately close the returned stream - we do not need it.
        this.fs.create(new Path(streamSegmentName),
                new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE),
                false,
                0,
                this.serviceBuilderConfig.getReplication(),
                this.serviceBuilderConfig.getBlockSize(),
                null).close();

        return new HDFSSegmentHandle(null, streamSegmentName); // null -> real Segment Name
    }

    private SegmentProperties getStreamSegmentInfoSync(String streamSegmentName) throws IOException {
        FileStatus[] status = this.fs.globStatus(new Path(streamSegmentName));
        return new StreamSegmentInformation(streamSegmentName,
                status[0].getLen(),
                status[0].getPermission().getUserAction() == FsAction.READ,
                false,
                new Date(status[0].getModificationTime()));
    }

    private void writeSync(String streamSegmentName, long offset, int length, InputStream data) throws Exception {
        try (FSDataOutputStream stream = this.fs.append(new Path(streamSegmentName))) {
            if (stream.getPos() != offset) {
                throw new BadOffsetException(streamSegmentName, offset, stream.getPos());
            }

            IOUtils.copyBytes(data, stream, length);
            stream.flush();
        }
    }

    private SegmentProperties sealSync(String streamSegmentName) throws IOException {
        this.fs.setPermission(
                new Path(streamSegmentName),
                new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));

        return getStreamSegmentInfoSync(streamSegmentName);
    }

    private void concatSync(String targetStreamSegmentName, long offset, String sourceStreamSegmentName) throws Exception {
        FileStatus status = list(new Path(targetStreamSegmentName))[0];
        if (status.getLen() != offset) {
            throw new BadOffsetException(targetStreamSegmentName, offset, status.getLen());
        }

        this.fs.concat(
                new Path(targetStreamSegmentName),
                new Path[]{new Path(targetStreamSegmentName), new Path(sourceStreamSegmentName)});
    }

    /**
     * Finds the file containing the given offset for the given segment.
     * Reads from that file.
     */
    private int readSync(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length) throws IOException {
        try (FSDataInputStream stream = this.fs.open(new Path(streamSegmentName))) {
            return stream.read(offset, buffer, bufferOffset, length);
        }
    }

    private boolean existsSync(String streamSegmentName) throws IOException {
        return this.fs.exists(new Path(streamSegmentName));
    }

    private void deleteSync(String name) throws IOException {
        this.fs.delete(new Path(name), false);
    }

    //endregion

    //region Helpers

    /**
     * Initializes the HDFSLowerStorage adapter. This operation is required prior to all other operations.
     *
     * @throws IOException If the adapter could not be initialized.
     */
    void initialize() throws IOException {
        Preconditions.checkState(this.fs == null, "HDFSLowerStorage has already been initialized.");
        this.conf = createFromConf(serviceBuilderConfig);
        this.fs = FileSystem.get(conf);
    }

    /**
     * Returns an array of FileStatus for all files matching the given pattern.
     *
     * @param targetPattern The pattern to match.
     * @throws IOException If the operation failed.
     */
    FileStatus[] list(Path targetPattern) throws IOException {
        ensureInitializedAndNotClosed();
        return this.fs.globStatus(targetPattern);
    }

    /**
     * Renames a file.
     *
     * @param source The source file.
     * @param target The target file (to be renamed to).
     * @throws IOException If the operation failed.
     */
    void rename(Path source, Path target) throws IOException {
        this.fs.rename(source, target);
    }

    private Configuration createFromConf(HDFSStorageConfig serviceBuilderConfig) {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", serviceBuilderConfig.getHDFSHostURL());
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return conf;
    }

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.fs != null, "HDFSLowerStorage is not initialized.");
    }

    CompletableFuture<Void> runAsync(RunnableWithException syncCode, String streamSegmentName) {
        ensureInitializedAndNotClosed();
        return CompletableFuture.runAsync(() -> {
            try {
                syncCode.run();
            } catch (Exception e) {
                throw new CompletionException(toStreamingException(streamSegmentName, e));
            }
        }, this.executor);
    }

    private <T> CompletableFuture<T> supplyAsync(Callable<T> syncCode, String streamSegmentName) {
        ensureInitializedAndNotClosed();
        return CompletableFuture.supplyAsync(() -> {
            try {
                return syncCode.call();
            } catch (Exception e) {
                throw new CompletionException(toStreamingException(streamSegmentName, e));
            }
        }, this.executor);
    }

    private static Throwable toStreamingException(String streamSegmentName, Throwable e) {
        if (e instanceof PathNotFoundException) {
            e = new StreamSegmentNotExistsException(streamSegmentName, e);
        } else if (e instanceof FileAlreadyExistsException) {
            e = new StreamSegmentExistsException(streamSegmentName, e);
        } else if (e instanceof AclException) {
            e = new StreamSegmentSealedException(streamSegmentName, e);
        } else if (!(e instanceof StreamingException)) {
            e = new StorageException("General error while performing HDFS operation.", e);
        }

        return e;
    }

    //endregion
}
