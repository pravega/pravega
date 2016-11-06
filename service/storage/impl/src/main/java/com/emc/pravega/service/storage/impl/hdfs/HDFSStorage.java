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
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.storage.SegmentHandle;
import com.emc.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Higher level HDFSStorage which does lock implementation based on file permissions.
 * Each Segment is represented by a file with pattern <segment-name>_<owner_host_id>
 * Owner_host_id is optional and means that the segment is owned by the Pravega host with the given id.
 * <p>
 * Whenever ownership change happens, the new node renames the file representing the segment to <segment-name>_<owner_host_id>.
 * This is done by the acquireLockForSegment call.
 * <p>
 * When a segment is sealed, it is renamed to its absolute name "segment-name" and marked as read-only.
 */
@Slf4j
public class HDFSStorage implements Storage {
    //region Members

    private final HDFSStorageConfig serviceBuilderConfig;
    private final HDFSLowerStorage storage;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSStorage class.
     *
     * @param serviceBuilderConfig The configuration to use.
     * @param executor             The Executor to use.
     */
    public HDFSStorage(HDFSStorageConfig serviceBuilderConfig, Executor executor) {
        Preconditions.checkNotNull(serviceBuilderConfig, "serviceBuilderConfig");
        Preconditions.checkNotNull(executor, "executor");
        this.serviceBuilderConfig = serviceBuilderConfig;
        this.storage = new HDFSLowerStorage(serviceBuilderConfig, executor);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.storage.close();
    }

    //endregion

    //region Storage Implementation

    @Override
    public CompletableFuture<SegmentHandle> create(String streamSegmentName, Duration timeout) {
        return this.storage.create(getOwnedSegmentFullPath(streamSegmentName), timeout)
                           .thenApply(handle -> changeName((HDFSSegmentHandle) handle, streamSegmentName));
    }

    @Override
    public CompletableFuture<Void> acquireLockForSegment(String streamSegmentName) {
        return this.storage.runAsync(() -> acquireLockForSegmentSync(streamSegmentName), streamSegmentName);
    }

    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
        return this.storage.write(getOwnedSegmentFullPath(streamSegmentName), offset, data, length, timeout);
    }

    @Override
    public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
        return this.storage.seal(getOwnedSegmentFullPath(streamSegmentName), timeout);
    }

    @Override
    public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset, String sourceStreamSegmentName, Duration timeout) {
        return this.storage.concat(getOwnedSegmentFullPath(targetStreamSegmentName), offset, getOwnedSegmentFullPath(sourceStreamSegmentName), timeout);
    }

    @Override
    public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
        return this.storage.delete(getOwnedSegmentFullPath(streamSegmentName), timeout);
    }

    @Override
    public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return this.storage.read(getOwnedSegmentFullPath(streamSegmentName), offset, buffer, bufferOffset, length, timeout);
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return this.storage.getStreamSegmentInfo(getOwnedSegmentFullPath(streamSegmentName), timeout)
                           .thenApply(properties -> changeName(properties, streamSegmentName));
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return this.storage.exists(getOwnedSegmentFullPath(streamSegmentName), timeout);
    }

    //endregion

    //region Helpers

    /**
     * Initializes the HDFSStorage.
     *
     * @throws IOException If the initialization failed.
     */
    public void initialize() throws IOException {
        this.storage.initialize();
    }

    /**
     * Returns a new SegmentProperties with the name of the segment changed to the given one.
     *
     * @param properties        Actual segment properties.
     * @param streamSegmentName The real name of the segment.
     */
    private SegmentProperties changeName(SegmentProperties properties, String streamSegmentName) {
        return new StreamSegmentInformation(streamSegmentName,
                properties.getLength(),
                properties.isSealed(),
                properties.isDeleted(),
                properties.getLastModified());
    }

    private SegmentHandle changeName(HDFSSegmentHandle handle, String streamSegmentName) {
        return new HDFSSegmentHandle(streamSegmentName, handle.getPhysicalSegmentName());
    }

    /**
     * Gets the full name of the file representing the segment which is locked by the current Pravega host.
     */
    private String getOwnedSegmentFullPath(String streamSegmentName) {
        return getCommonPartOfName(streamSegmentName) + "_" + this.serviceBuilderConfig.getPravegaId();
    }

    private String getCommonPartOfName(String streamSegmentName) {
        return this.serviceBuilderConfig.getHdfsRoot() + "/" + streamSegmentName;
    }

    /**
     * Attempts to take over ownership of a segment.
     * <ol>
     * <li> Find the file with the biggest start offset.
     * <li> Create a new file with the name equal to the current offset of the stream which is biggest start offset + its size.
     * </ol>
     */
    private void acquireLockForSegmentSync(String streamSegmentName) throws IOException, StreamSegmentNotExistsException {
        FileStatus[] statuses = this.storage.list(new Path(getCommonPartOfName(streamSegmentName) + "_[0-9]*"));

        if (statuses.length != 1) {
            throw new StreamSegmentNotExistsException(streamSegmentName);
        }

        this.storage.rename(statuses[0].getPath(), new Path(this.getOwnedSegmentFullPath(streamSegmentName)));
    }

    //endregion
}
