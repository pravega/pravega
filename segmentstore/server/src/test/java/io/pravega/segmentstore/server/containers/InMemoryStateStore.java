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

import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.util.AsyncMap;
import io.pravega.common.util.ByteArraySegment;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;

import lombok.SneakyThrows;
import lombok.val;

/**
 * In-memory mock for AsyncMap.
 */
@ThreadSafe
public class InMemoryStateStore implements AsyncMap<String, SegmentState> {
    private final ConcurrentHashMap<String, ByteArraySegment> map = new ConcurrentHashMap<>();

    @Override
    @SneakyThrows(IOException.class)
    public CompletableFuture<Void> put(String segmentName, SegmentState state, Duration timeout) {
        val innerStream = new EnhancedByteArrayOutputStream();
        val stream = new DataOutputStream(innerStream);
        state.serialize(stream);
        stream.flush();
        this.map.put(segmentName, innerStream.getData());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @SneakyThrows(IOException.class)
    public CompletableFuture<SegmentState> get(String segmentName, Duration timeout) {
        ByteArraySegment s = this.map.getOrDefault(segmentName, null);
        if (s == null) {
            // No state saved.
            return CompletableFuture.completedFuture(null);
        } else {
            return CompletableFuture.completedFuture(SegmentState.deserialize(new DataInputStream(s.getReader())));
        }
    }

    @Override
    public CompletableFuture<Void> remove(String key, Duration timeout) {
        this.map.remove(key);
        return CompletableFuture.completedFuture(null);
    }
}
