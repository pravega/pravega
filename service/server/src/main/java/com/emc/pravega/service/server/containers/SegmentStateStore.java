/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.TimeoutTimer;
import com.emc.pravega.common.io.EnhancedByteArrayOutputStream;
import com.emc.pravega.common.segment.StreamSegmentNameUtils;
import com.emc.pravega.common.util.AsyncMap;
import com.emc.pravega.common.util.ByteArraySegment;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.SneakyThrows;

/**
 * Stores and Retrieves Segment Attribute values.
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
        String stateSegment = StreamSegmentNameUtils.getStateSegmentName(segmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.storage
                .getStreamSegmentInfo(stateSegment, timer.getRemaining())
                .thenComposeAsync(sp -> readSegmentState(sp, timer.getRemaining()), this.executor)
                .exceptionally(this::handleSegmentNotExistsException);
    }

    @Override
    public CompletableFuture<Void> put(String segmentName, SegmentState state, Duration timeout) {
        String stateSegment = StreamSegmentNameUtils.getStateSegmentName(segmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        ByteArraySegment toWrite = serialize(state);

        // We need to replace the contents of the Segment. The only way to do that with the Storage API is to
        // delete the existing segment (if any), then create a new one and write the contents to it.
        return this.storage
                .delete(stateSegment, timer.getRemaining())
                .exceptionally(this::handleSegmentNotExistsException)
                .thenComposeAsync(v -> this.storage.create(stateSegment, timer.getRemaining()), this.executor)
                .thenComposeAsync(
                        v -> this.storage.write(stateSegment, 0, toWrite.getReader(), toWrite.getLength(), timer.getRemaining()),
                        this.executor);
    }

    @Override
    public CompletableFuture<Void> remove(String segmentName, Duration timeout) {
        String stateSegment = StreamSegmentNameUtils.getStateSegmentName(segmentName);
        return this.storage
                .delete(stateSegment, timeout)
                .exceptionally(this::handleSegmentNotExistsException);
    }

    //endregion

    //region Helpers

    private CompletableFuture<SegmentState> readSegmentState(SegmentProperties stateSegmentInfo, Duration timeout) {
        byte[] contents = new byte[(int) stateSegmentInfo.getLength()];
        return this.storage
                .read(stateSegmentInfo.getName(), 0, contents, 0, contents.length, timeout)
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
        if (ex instanceof StreamSegmentNotExistsException) {
            // It's ok if the state segment does not exist.
            return null;
        }

        throw ex;
    }

    //endregion
}
