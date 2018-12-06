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

import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link MetadataStore} implementation that stores all Segment Information as individual files in a {@link Storage}.
 * These individual files are called Segment State Files and their names are obtained via
 * {@link StreamSegmentNameUtils#getStateSegmentName}.
 */
@Slf4j
public class StorageMetadataStore extends MetadataStore {
    private final Storage storage;

    /**
     * Creates a new instance of the {@link StorageMetadataStore} class.
     *
     * @param connector A Connector object that can be used to communicate between the Metadata Store and upstream callers.
     * @param storage   The {@link Storage} to use for storing Segment State Files.
     * @param executor  The executor to use for async operations.
     */
    public StorageMetadataStore(Connector connector, @NonNull Storage storage, Executor executor) {
        super(connector, executor);
        this.storage = storage;
    }

    /**
     * Attempts to create the given Segment in this Metadata Store, with possible recovery from a previous incomplete
     * attempt. When this method completes successfully, the State File will have been created for this Segment, but not
     * the segment itself (the Segment will be created upon its first write).
     *
     * The recovery part handles these three major cases:
     * <ul>
     * <li>State File exists and has contents: the operation will fail with {@link StreamSegmentExistsException}.
     * <li>State File exists, has a length of zero: the State File will be recreated using the given segmentInfo and the
     * operation will complete successfully (pending a successful State File creation and write).
     * </ul>
     *
     * @param segmentName The name of the Segment to create.
     * @param segmentInfo An {@link ArrayView} containing the serialized Segment Info to store for this segment.
     * @param timer       A TimeoutTimer to determine how much time is left to complete the operation.
     * @return A CompletableFuture that, when completed, will indicate that the Segment has been successfully created.
     */
    @Override
    protected CompletableFuture<Void> createSegment(String segmentName, ArrayView segmentInfo, TimeoutTimer timer) {
        // Create the State Segment, then write the contents to it. We need to handle the case where a previous attempt only
        // managed to create the State Segment but wasn't able to write anything to it. As such, we treat empty State Segments
        // as inexistent.
        String stateSegment = StreamSegmentNameUtils.getStateSegmentName(segmentName);
        return Futures.exceptionallyCompose(
                Futures.toVoid(this.storage.create(stateSegment, SegmentRollingPolicy.NO_ROLLING, timer.getRemaining())),
                ex -> handleCreateException(stateSegment, Exceptions.unwrap(ex), timer))
                      .thenComposeAsync(v -> writeSegmentInfo(stateSegment, segmentInfo, timer.getRemaining()), this.executor);
    }

    @Override
    public CompletableFuture<Void> clearSegmentInfo(String segmentName, Duration timeout) {
        String stateSegment = StreamSegmentNameUtils.getStateSegmentName(segmentName);
        return this.storage
                .openWrite(stateSegment)
                .thenComposeAsync(handle -> this.storage.delete(handle, timeout), this.executor)
                .exceptionally(this::handleSegmentNotExistsException);
    }

    @Override
    protected CompletableFuture<Void> updateSegmentInfo(String segmentName, ArrayView segmentInfo, Duration timeout) {
        String stateSegment = StreamSegmentNameUtils.getStateSegmentName(segmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return clearSegmentInfo(segmentName, timer.getRemaining())
                .thenComposeAsync(v -> this.storage.create(stateSegment, SegmentRollingPolicy.NO_ROLLING, timer.getRemaining()), this.executor)
                .thenComposeAsync(v -> writeSegmentInfo(stateSegment, segmentInfo, timer.getRemaining()), this.executor);
    }

    /**
     * Gets information about a StreamSegment as it exists in Storage and in the State Store.
     *
     * @param segmentName The case-sensitive StreamSegment Name.
     * @param timeout     Timeout for the Operation.
     * @return A CompletableFuture that, when complete, will contain a SegmentProperties object with the desired
     * information. If failed, it will contain the exception that caused the failure.
     */
    @Override
    protected CompletableFuture<ArrayView> getSegmentInfoInternal(String segmentName, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        String stateSegment = StreamSegmentNameUtils.getStateSegmentName(segmentName);
        return this.storage
                .getStreamSegmentInfo(stateSegment, timer.getRemaining())
                .thenComposeAsync(sp -> {
                    if (sp.getLength() == 0) {
                        // Empty state files are treated the same as if they didn't exist.
                        return Futures.failedFuture(new StreamSegmentNotExistsException(stateSegment));
                    } else {
                        return readStateSegment(sp, timer.getRemaining());
                    }
                }, this.executor);
    }

    private CompletableFuture<Void> writeSegmentInfo(String stateSegment, ArrayView segmentInfo, Duration timeout) {
        return this.storage
                .openWrite(stateSegment)
                .thenComposeAsync(handle -> this.storage.write(handle, 0, segmentInfo.getReader(), segmentInfo.getLength(), timeout),
                        this.executor);
    }

    /**
     * Exception handler in the case when a Segment fails to be created in Storage. This only handles
     * StreamSegmentExistsException; all other exceptions are "bubbled" up automatically.
     *
     * This method simply checks the integrity of the State File; if it exists and is valid, then the Segment is considered
     * fully created and the original exception is bubbled up. If the State File does not exist or it is not valid, then
     * the most up-to-date information about the segment is returned (as it exists in Storage).
     *
     * @param stateSegmentName  The name of the Segment involved.
     * @param originalException The exception that triggered this.
     * @param timer             A TimeoutTimer to determine how much time is left to complete the operation.
     * @return A CompletableFuture that, when completed normally, will contain information about the Segment. If the Segment
     * already exists or another error happened, this will be completed with the appropriate exception.
     */
    private CompletableFuture<Void> handleCreateException(String stateSegmentName, Throwable originalException, TimeoutTimer timer) {
        if (!(originalException instanceof StreamSegmentExistsException)) {
            // Some other kind of exception that we can't handle here.
            return Futures.failedFuture(originalException);
        }

        return this.storage
                .getStreamSegmentInfo(stateSegmentName, timer.getRemaining())
                .exceptionally(ex -> {
                    ex = Exceptions.unwrap(ex);
                    if (ex instanceof StreamSegmentNotExistsException) {
                        // The State File is missing. We have the data needed to rebuild it, so ignore any exceptions coming this way.
                        log.warn("{}: Missing State File '{}'; recreating.", this.traceObjectId, stateSegmentName, ex);
                        return null;
                    }

                    // All other exceptions need to be bubbled up.
                    throw new CompletionException(ex);
                })
                .thenAccept(si -> {
                    if (si.getLength() > 0) {
                        // State file already exists and has data; re-throw original exception.
                        throw new CompletionException(originalException);
                    }
                });
    }

    @SneakyThrows(Throwable.class)
    private <T> T handleSegmentNotExistsException(Throwable ex) {
        ex = Exceptions.unwrap(ex);
        if (ex instanceof StreamSegmentNotExistsException) {
            // It's ok if the state segment does not exist.
            return null;
        }

        throw ex;
    }

    private CompletableFuture<ArrayView> readStateSegment(SegmentProperties stateSegmentInfo, Duration timeout) {
        byte[] contents = new byte[(int) stateSegmentInfo.getLength()];
        return this.storage
                .openRead(stateSegmentInfo.getName())
                .thenComposeAsync(handle -> this.storage.read(handle, 0, contents, 0, contents.length, timeout), this.executor)
                .thenApply(bytesRead -> {
                    assert bytesRead == contents.length : "Expected to read " + contents.length + " bytes, read " + bytesRead;
                    return new ByteArraySegment(contents);
                });
    }
}
