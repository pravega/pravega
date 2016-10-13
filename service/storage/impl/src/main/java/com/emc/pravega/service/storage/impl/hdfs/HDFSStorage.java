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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 *
 * higher level HDFSStorage which does lock implementation based on file permissions.
 * Each segment is represented by a file with pattern <segment-name>_<startoffset>
 * Start offset represents the offset in the segment of the first byte of the given file.
 *
 * When ever ownership change happens, the new node marks all the earlier files representing the segment
 * readonly. Creates a new file with the current offset as part of the name. This is done in the acquireLockForSegment code.
 *
 * */
@Slf4j
public class HDFSStorage implements Storage {

    private final Executor executor;
    private final HDFSStorageConfig serviceBuilderConfig;
    private final HDFSLowerStorage storage;


    public HDFSStorage(HDFSStorageConfig serviceBuilderConfig, Executor executor) {
        this.serviceBuilderConfig = serviceBuilderConfig;
        this.executor  = executor;
        this.storage   = new HDFSLowerStorage(serviceBuilderConfig, executor);
    }



    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return storage.create(getFirstSegmentFullPath(streamSegmentName), timeout).
                thenApply(properties -> changeNameToStream(properties, streamSegmentName));
    }

    /**
     * Utility function to change the name in the SegmentProperties from the last file
     * to the segment name
     * @param properties             Properties representing the segment name
     * @param streamSegmentName      The name of the segment
     */
    private SegmentProperties changeNameToStream(SegmentProperties properties, String streamSegmentName) {
        return new StreamSegmentInformation(streamSegmentName,
                properties.getLength(),
                properties.isSealed(),
                properties.isDeleted(),
                properties.getLastModified());
    }

    /**
     * Utility function to get the full name of the first file representing the segment.
     */
    private String getFirstSegmentFullPath(String streamSegmentName) {
        return getSegmentFullPathStartingAtOffset(streamSegmentName, 0);
    }

    /**
     * Utility function to get the full name of a file representing part of the segment. The
     * data in the file starts at a given offset
     */
    private String getSegmentFullPathStartingAtOffset(String streamSegmentName, long currStart) {
        return getCommonPartOfName(streamSegmentName) + "_" + currStart;
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

    /**
     * Utility function that returns full path name of the file which actually contains
     * data at the given offset for the segment
     */
    private String getSegmentFullPathContainingOffset(String streamSegmentName, long offset) throws IOException {
        String possibleValForAppend = null;
        for (FileStatus status: getStreamSegmentNameWildCard(streamSegmentName)) {
            long fileStartOffset = this.getStartOffsetInName(status.getPath().toString());
            if (fileStartOffset <= offset && fileStartOffset + status.getLen() < offset) {
                return status.getPath().toString();
            }
            if (fileStartOffset <= offset && fileStartOffset + status.getLen() <= offset) {
                possibleValForAppend = status.getPath().toString();
            }
        }
        return possibleValForAppend;
    }

    /**
     * Utility function that given name of a file representing a segment,
     * returns the start offset of the data in the file
     */
    private long getStartOffsetInName(String currFilename) {
        if (currFilename == null) return 0;
        String[] tokens = currFilename.split("_");
        String sizeStr = tokens[tokens.length -1];
        return Long.valueOf(sizeStr);
    }


    @Override
    public CompletableFuture<Boolean> acquireLockForSegment(String streamSegmentName) {

        CompletableFuture<Boolean> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
                    try {
                        retVal.complete(acquireLockForSegmentSync(streamSegmentName));
                    } catch (IOException e) {
                        retVal.completeExceptionally(e);
                    }
                },
                executor);
        return retVal;
    }

    /**
     * Algorithm to take over the ownership of a segment.
     *
     * List the files.
     * Mark all the files readonly
     * Find the file with the biggest start offset
     * Create a new file with the name equal to
     * the current offset of the stream which is biggest start offset + its size
     */
    private Boolean acquireLockForSegmentSync(String streamSegmentName) throws IOException {


        FileStatus[] statuses = this.getStreamSegmentNameWildCard(streamSegmentName);

        final long[] currStart = {0};
        final SegmentProperties[] lastProp = {null};
        Arrays.stream(statuses).map(status -> status.getPath().toString()).
                map(name -> {
                    SegmentProperties props;
                    props = (SegmentProperties) storage.seal(name, null);
                    long currVal = this.getStartOffsetInName(name);
                    if (currVal > currStart[0]) {
                        currStart[0] = currVal;
                        lastProp[0] = props;
                    }
                    return props;
                }).count();

       currStart[0] = currStart[0] + lastProp[0].getLength();
        storage.createSync(getSegmentFullPathStartingAtOffset(streamSegmentName, currStart[0]), null);
        return true;
    }

    /**
     * Sets all the file belonging to a segment readonly.
     */
    private SegmentProperties setAllFilesReadOnlySync(String streamSegmentName) throws IOException {
        //List the files.
        //Mark all the files readonly

        FileStatus[] statuses = this.getStreamSegmentNameWildCard(streamSegmentName);

        final long[] currStart = {0};
        final SegmentProperties[] lastProp = {null};
        Arrays.stream(statuses).map(status -> status.getPath().toString()).
                map(name -> {
                    SegmentProperties props;
                    try {
                        props = storage.sealSync(name, null);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    long currVal = this.getStartOffsetInName(name);
                    if (currVal > currStart[0]) {
                        currStart[0] = currVal;
                        lastProp[0] = props;
                    }
                    return props;
                }).count();

        return new StreamSegmentInformation(streamSegmentName,
                currStart[0] + lastProp[0].getLength(),
                true,
                false,
                lastProp[0].getLastModified());
    }


    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {

        return CompletableFuture.supplyAsync(() -> {
            try {
                return this.getSegmentFullPathContainingOffset(streamSegmentName, offset);
            } catch (IOException e) {
                //TBD Log error
                return null;
            }
        }, executor).
                thenAcceptAsync(
                currFilename -> storage.write(currFilename,
                        offset - this.getStartOffsetInName(currFilename),
                        data, length, timeout)
                );
    }


    @Override
    public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {

        CompletableFuture<SegmentProperties> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync( () -> {
            try {
                retVal.complete(this.setAllFilesReadOnlySync(streamSegmentName));
            } catch (IOException e) {
                retVal.completeExceptionally(e);
            }
        }, executor);
        return retVal;
    }


    @Override
    public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset, String sourceStreamSegmentName, Duration timeout) {
        CompletableFuture<Void> retVal = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                retVal.complete(concatSync(
                        this.getSegmentFullPathContainingOffset(targetStreamSegmentName, offset),
                        offset - this.getStartOffsetInName(
                                this.getSegmentFullPathContainingOffset(targetStreamSegmentName, offset)),
                        this.getFirstSegmentFullPath(sourceStreamSegmentName),
                        timeout));
            } catch (Exception e) {
                retVal.completeExceptionally(e);
            }
        }, executor);
        return retVal;
    }


    private Void concatSync(String targetStreamSegmentName, long offset, String sourceStreamSegmentName, Duration timeout) throws IOException, BadOffsetException {
        return storage.concatSync(targetStreamSegmentName, offset, sourceStreamSegmentName, timeout);
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

    /**
     * Deletes all the files belonging to the given stream segment
     */
    private Void deleteSync(String streamSegmentName, Duration timeout) throws IOException {
        FileStatus[] statuses = this.getStreamSegmentNameWildCard(streamSegmentName);
        Arrays.stream(statuses).map(status -> status.getPath().toString()).
                map(name -> {
                    try {
                        storage.deleteSync(name, null);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return null;
                }).count();

        return null;
    }

    @Override
    public void close() {
        storage.close();
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
        String fileName = this.getSegmentFullPathContainingOffset(streamSegmentName, offset);
        FSDataInputStream stream = storage.getFS().open(new Path(fileName));
        return stream.read(offset- this.getStartOffsetInName(fileName),
                buffer, bufferOffset, length);
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

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        return storage.exists(this.getFirstSegmentFullPath(streamSegmentName), timeout);
     }


     /**
      *
      * Utility function that lists all the files and creates
      * the segment info based on the status of the file containing the largest offset
      */
    private SegmentProperties getStreamSegmentInfoSync(String streamSegmentName, Duration timeout) throws IOException {
        FileStatus[] statuses = this.getStreamSegmentNameWildCard(streamSegmentName);

        long currStart = 0;
        SegmentProperties lastProp = null;

        for (FileStatus status: statuses) {
            String name = status.getPath().toString();
            SegmentProperties props;
            try {
               props = storage.getStreamSegmentInfoSync(name, null);
            } catch (IOException e) {
            throw new UncheckedIOException(e);
            }
            long currVal = this.getStartOffsetInName(name);
            if (currVal > currStart || lastProp == null) {
                currStart = currVal;
                lastProp = props;
            }
         }

        return new StreamSegmentInformation(streamSegmentName,
                currStart + lastProp.getLength(),
                lastProp.isSealed(),
                lastProp.isDeleted(),
                lastProp.getLastModified());
    }

}
