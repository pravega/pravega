/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import io.pravega.common.ExceptionHelpers;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.util.AsyncMap;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Stores and Retrieves Segment Attribute values.
 *
 * Each state is represented by at most two files. At least one of these will always be carrying valid data.
 *
 * A call to put() will:
 *  1. If more than one statefiles exist:
 *      a. Validate that the latest one has correct data.
 *      b. Delete the older statefile if the latest one has valid data.
 *  2. Create a statefile with the same name as the one deleted and write state to it.
 *  3. Delete the existing statefile.
 *
 *  A call to get() will:
 *  1. Get the latest statefile.
 *  2. Try to read from it, if this read fails, read from the older statefile.
 *
 * <p>
 * Expected concurrency behavior:
 * <ul>
 * <li> Concurrent calls to any method with different Keys (SegmentName) will work without issue.
 * <li> Concurrent calls to get() with the same key will work without issue.
 * <li> Concurrent calls to put() with the same key will result in one call succeeding and the others failing.
 * <li> Concurrent calls to remove() with the same key will work without issue.
 * <li> Concurrent calls to put() and remove() with the same key will either both succeed (in which case the outcome is
 * undefined) or the remove() will succeed and put() will fail with StreamSegmentNotExistsException (in which case the
 * call to put() has been neutered by the call to remove()).
 * </ul>
 */
@Slf4j
class SegmentStateStore implements AsyncMap<String, SegmentState> {
    //region Members

    private final Storage storage;
    private final Executor executor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SegmentStateStore class.
     *
     * @param storage  The Storage to use.
     * @param executor The Executor to use for asynchronous operations.
     */
    SegmentStateStore(Storage storage, Executor executor) {
        Preconditions.checkNotNull(storage, "storage");
        Preconditions.checkNotNull(executor, "executor");
        this.storage = storage;
        this.executor = executor;
    }

    //endregion

    //region AsyncMap implementation

    @Override
    public CompletableFuture<SegmentState> get(String segmentName, Duration timeout) {
        return findLatestValidState(segmentName, timeout);
    }

    private CompletableFuture<SegmentState> findLatestValidState(String segmentName, Duration timeout) {
        String stateSegment1 = StreamSegmentNameUtils.getFirstStateSegmentName(segmentName);
        String stateSegment2 = StreamSegmentNameUtils.getSecondStateSegmentName(segmentName);

        final CompletableFuture<SegmentState>[] states = new CompletableFuture[2];
        final SegmentProperties[] props = new SegmentProperties[2];

        TimeoutTimer timer = new TimeoutTimer(timeout);

        CompletableFuture<SegmentState> retVal1 = this.storage
                .getStreamSegmentInfo(stateSegment1, timer.getRemaining())
                .thenComposeAsync(sp -> {
                    props[0] = sp;
                    states[0] = readSegmentState(sp, timer.getRemaining())
                            .exceptionally(this::handleSegmentNotExistsException);
                    return states[0];
                }, this.executor)
                .exceptionally(this::handleSegmentNotExistsException);

        CompletableFuture<SegmentState> retVal2 = this.storage
                .getStreamSegmentInfo(stateSegment2, timer.getRemaining())
                .thenComposeAsync(sp -> {
                    props[1] = sp;
                    states[1] = readSegmentState(sp, timer.getRemaining())
                            .exceptionally(this::handleSegmentNotExistsException);
                    return states[1];
                }, this.executor)
                .exceptionally(this::handleSegmentNotExistsException);

        return CompletableFuture.allOf(retVal1, retVal2).thenApplyAsync((v) -> {
            SegmentState s1 = states[0] == null ? null : states[0].join();
            SegmentState s2 = states[1] == null ? null : states[1].join();

            if (s1 == null) {
                return s2;
            }
            if (s2 == null) {
                return s1;
            }
            if (props[0].getLastModified().asDate().compareTo(props[1].getLastModified().asDate()) > 0) {
                return s1;
            } else {
                return s2;
            }
        }, this.executor);
    }

    private CompletableFuture<String> findInvalidOrOlderState(String segmentName, Duration timeout) {
        String stateSegment1 = StreamSegmentNameUtils.getFirstStateSegmentName(segmentName);
        String stateSegment2 = StreamSegmentNameUtils.getSecondStateSegmentName(segmentName);

        final CompletableFuture<SegmentState>[] states = new CompletableFuture[2];
        final SegmentProperties[] props = new SegmentProperties[2];

        TimeoutTimer timer = new TimeoutTimer(timeout);

        CompletableFuture<SegmentState> retVal1 = this.storage
                .getStreamSegmentInfo(stateSegment1, timer.getRemaining())
                .thenComposeAsync(sp -> {
                    props[0] = sp;
                    states[0] = readSegmentState(sp, timer.getRemaining())
                            .exceptionally(this::handleSegmentNotExistsException);
                    return states[0];
                }, this.executor)
                .exceptionally(this::handleSegmentNotExistsException);

        CompletableFuture<SegmentState> retVal2 = this.storage
                .getStreamSegmentInfo(stateSegment2, timer.getRemaining())
                .thenComposeAsync(sp -> {
                    props[1] = sp;
                    states[1] = readSegmentState(sp, timer.getRemaining())
                            .exceptionally(this::handleSegmentNotExistsException);
                    return states[1];
                }, this.executor)
                .exceptionally(this::handleSegmentNotExistsException);

        return CompletableFuture.allOf(retVal1, retVal2).thenApplyAsync((v) -> {
            SegmentState s1 = states[0] == null ? null : states[0].join();
            SegmentState s2 = states[1] == null ? null : states[1].join();

            if (s1 == null) {
                return stateSegment1;
            }
            if (s2 == null) {
                return stateSegment2;
            }
            if (props[0].getLastModified().asDate().compareTo(props[1].getLastModified().asDate()) < 0) {
                return stateSegment1;
            } else {
                return stateSegment2;
            }
        }, this.executor);

    }

    @Override
    public CompletableFuture<Void> put(String segmentName, SegmentState state, Duration timeout) {
        String stateSegment1 = StreamSegmentNameUtils.getFirstStateSegmentName(segmentName);
        String stateSegment2 = StreamSegmentNameUtils.getSecondStateSegmentName(segmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        ByteArraySegment toWrite = serialize(state);

        // We need to replace the contents of the current statefile Segment.
        // The only way to do that with the Storage API is to
        // delete the existing segment (if any), then create a new one and write the contents to it.
        // In case a failover happens and this process is aborted with either no statefile or corrupt data,
        // we have a backup file with slightly older data.

        final String[] stateSegment = new String[1];
        CompletableFuture<Void> retVal = this.findInvalidOrOlderState(segmentName, timeout)
                                             .thenComposeAsync(name -> {
                                                 stateSegment[0] = name;
                                                 return this.storage.openWrite(name);
                                             }, this.executor)
                                             .thenComposeAsync(handle -> this.storage.delete(handle, timer.getRemaining()), this.executor)
                                             .exceptionally(this::handleSegmentNotExistsException)
                                             .thenComposeAsync(v -> this.storage.create(stateSegment[0], timer.getRemaining()), this.executor)
                                             .thenComposeAsync(v -> this.storage.openWrite(stateSegment[0]), this.executor)
                                             .thenComposeAsync(
                                                     handle -> this.storage.write(handle, 0, toWrite.getReader(), toWrite.getLength(), timer.getRemaining()),
                                                     this.executor);

        //Schedule the delete of older segment.
        retVal.thenComposeAsync(v -> {
                        String toBeDeleted = (stateSegment[0].equals(stateSegment1)) ? stateSegment2 : stateSegment1;
                        return this.storage.openWrite(toBeDeleted);
                        }, this.executor)
                .thenComposeAsync(handle -> this.storage.delete(handle, timeout), this.executor)
                .exceptionally(e -> {
                        log.info("Exception while deleting old segment");
                        return null;
                });

        return retVal;
    }

    @Override
    public CompletableFuture<Void> remove(String segmentName, Duration timeout) {
        String stateSegment1 = StreamSegmentNameUtils.getFirstStateSegmentName(segmentName);
        String stateSegment2 = StreamSegmentNameUtils.getSecondStateSegmentName(segmentName);
        CompletableFuture<Void> firstDelete = this.storage
                .openWrite(stateSegment1)
                .thenComposeAsync(handle -> this.storage.delete(handle, timeout), this.executor)
                .exceptionally(this::handleSegmentNotExistsException);
        CompletableFuture<Void> secondDelete = this.storage
                .openWrite(stateSegment2)
                .thenComposeAsync(handle -> this.storage.delete(handle, timeout), this.executor)
                .exceptionally(this::handleSegmentNotExistsException);

        return CompletableFuture.allOf(firstDelete, secondDelete);
    }

    //endregion

    //region Helpers

    private CompletableFuture<SegmentState> readSegmentState(SegmentProperties stateSegmentInfo, Duration timeout) {
        byte[] contents = new byte[(int) stateSegmentInfo.getLength()];
        return this.storage
                .openRead(stateSegmentInfo.getName())
                .thenComposeAsync(handle -> this.storage.read(handle, 0, contents, 0, contents.length, timeout), this.executor)
                .thenApplyAsync(bytesRead -> {
                    assert bytesRead == contents.length : "Expected to read " + contents.length + " bytes, read " + bytesRead;
                    return deserialize(contents);
                }, this.executor);
    }

    @SneakyThrows(IOException.class)
    private ByteArraySegment serialize(SegmentState state) {
        try (EnhancedByteArrayOutputStream innerStream = new EnhancedByteArrayOutputStream();
             DataOutputStream output = new DataOutputStream(innerStream)) {
            state.serialize(output);
            output.flush();
            return innerStream.getData();
        }
    }

    @SneakyThrows(IOException.class)
    private SegmentState deserialize(byte[] contents) {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(contents))) {
            return SegmentState.deserialize(input);
        }
    }

    @SneakyThrows(Throwable.class)
    private <T> T handleSegmentNotExistsException(Throwable ex) {
        ex = ExceptionHelpers.getRealException(ex);
        if (ex instanceof StreamSegmentNotExistsException || ex instanceof EOFException) {
            // It's ok if the state segment does not exist.
            return null;
        }

        throw ex;
    }

    //endregion
}
