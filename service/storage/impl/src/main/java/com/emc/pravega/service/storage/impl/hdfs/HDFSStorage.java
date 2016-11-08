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
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * higher level HDFSStorage which does lock implementation based on file permissions.
 * Each segment is represented by a file with pattern <segment-name>_<owner_host_id>
 * Owner_host_id is optional and means that the segment is owned by the Pravega host of the given id.
 * <p>
 * When ever ownership change happens, the new node renames the file representing the segment to <segment-name>_<owner_host_id>.
 * This is done by the open call.
 *
 * When a segment is sealed, it is renamed to its absolute name "segment-name" and marked as read-only.
 */
@Slf4j
public class HDFSStorage implements Storage {

    private final Executor executor;
    private final HDFSStorageConfig serviceBuilderConfig;
    private final HDFSLowerStorage storage;


    public HDFSStorage(HDFSStorageConfig serviceBuilderConfig, Executor executor) {
        this.serviceBuilderConfig = serviceBuilderConfig;
        this.executor = executor;
        this.storage = new HDFSLowerStorage(serviceBuilderConfig, executor);
    }


    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return storage.create(getOwnedSegmentFullPath(streamSegmentName), timeout).
                thenApply(properties -> changeNameInSegmentProperties(properties, streamSegmentName));
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
        return storage.getFS().globStatus(new Path(getCommonPartOfName(streamSegmentName) + "_" + "[0-9]*"));
    }

    private String getCommonPartOfName(String streamSegmentName) {
        return serviceBuilderConfig.getHdfsRoot() + "/" + streamSegmentName;
    }

    @Override
    public CompletableFuture<Void> open(String streamSegmentName) {
        return FutureHelpers.runAsyncTranslateException(() -> {
                    acquireLockForSegmentSync(streamSegmentName);
                    return null;
                },
                e -> HDFSExceptionHelpers.translateFromIOException(streamSegmentName, e),
                executor);
    }

    /**
     * Algorithm to take over the ownership of a segment.
     * <p>
     * List the files.
     * Mark all the files readonly
     * Find the file with the biggest start offset
     * Create a new file with the name equal to
     * the current offset of the stream which is biggest start offset + its size
     */
    private void acquireLockForSegmentSync(String streamSegmentName) throws IOException, StreamSegmentNotExistsException {

        FileStatus[] statuses = this.getStreamSegmentNameWildCard(streamSegmentName);

        if (statuses.length != 1) {
            throw new StreamSegmentNotExistsException(streamSegmentName);
        }
        storage.getFS().rename(statuses[0].getPath(), new Path(this.getOwnedSegmentFullPath(streamSegmentName)));
    }

    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
        return storage.write(this.getOwnedSegmentFullPath(streamSegmentName), offset, data, length, timeout);
    }


    @Override
    public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
        return this.storage.seal(this.getOwnedSegmentFullPath(streamSegmentName), timeout);
    }

    @Override
    public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset, String sourceStreamSegmentName, Duration timeout) {
        return storage.concat(this.getOwnedSegmentFullPath(targetStreamSegmentName), offset, this.getOwnedSegmentFullPath(sourceStreamSegmentName), timeout);
    }

    @Override
    public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
        return storage.delete(this.getOwnedSegmentFullPath(streamSegmentName), timeout);
    }

    @Override
    public void close() {
        storage.close();
    }

    @Override
    public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return storage.read(this.getOwnedSegmentFullPath(streamSegmentName), offset, buffer, bufferOffset, length, timeout);
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return storage.getStreamSegmentInfo(this.getOwnedSegmentFullPath(streamSegmentName), timeout).
                thenApply(properties -> this.changeNameInSegmentProperties(properties, streamSegmentName));
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return storage.exists(this.getOwnedSegmentFullPath(streamSegmentName), timeout);
    }

    public void initialize() throws IOException {
        storage.getFS();
    }
}
