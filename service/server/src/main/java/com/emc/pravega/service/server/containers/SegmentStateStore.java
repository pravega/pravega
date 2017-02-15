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

package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.TimeoutTimer;
import com.emc.pravega.common.io.EnhancedByteArrayOutputStream;
import com.emc.pravega.common.segment.StreamSegmentNameUtils;
import com.emc.pravega.common.util.AsyncMap;
import com.emc.pravega.common.util.ByteArraySegment;
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
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;

/**
 * Stores and Retrieves Segment Attribute values.
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
        AtomicReference<byte[]> contents = new AtomicReference<>();
        return this.storage
                .getStreamSegmentInfo(stateSegment, timer.getRemaining())
                .thenComposeAsync(sp -> {
                    // Read the whole Segment.
                    contents.set(new byte[(int) sp.getLength()]);
                    return this.storage.read(stateSegment, 0, contents.get(), 0, contents.get().length, timer.getRemaining());
                }, this.executor)
                .thenApplyAsync(bytesRead -> {
                    assert bytesRead == contents.get().length : "Expected to read " + contents.get().length + " bytes, read " + bytesRead;
                    return deserialize(contents.get());
                }, this.executor)
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
